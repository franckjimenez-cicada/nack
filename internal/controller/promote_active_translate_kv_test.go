package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// Test_translateKeyValueSpecToPrimary asserts the inverse translation drops the
// mirror + sources and keeps the authored bucket config, without mutating the
// input — the KeyValue analog of Test_translateStreamSpecToPrimary.
func Test_translateKeyValueSpecToPrimary(t *testing.T) {
	orig := &api.KeyValueSpec{
		Bucket:  "B",
		History: 5,
		TTL:     "1h",
		Mirror:  &api.StreamSource{Name: kvStreamPrefix + "B"},
		Sources: []*api.StreamSource{{Name: "OTHER"}},
	}
	out := translateKeyValueSpecToPrimary(orig)
	assert.Nil(t, out.Mirror, "mirror must be stripped")
	assert.Nil(t, out.Sources, "sources must be stripped")
	assert.Equal(t, "B", out.Bucket, "authored bucket must be kept")
	assert.EqualValues(t, 5, out.History, "authored history must be kept")
	assert.Equal(t, "1h", out.TTL, "authored ttl must be kept")
	assert.NotNil(t, orig.Mirror, "input CR spec must be untouched (deep copy)")
	assert.NotNil(t, orig.Sources, "input CR spec sources must be untouched (deep copy)")
}

// Test_activeRoleTranslation_reconcilePromotesMirrorBaselineKV is the KeyValue
// companion to Test_activeRoleTranslation_reconcilePromotesMirrorBaselineCR (the
// stream prod scenario, PR #17). The gitops "mirror baseline" KV CR carries BOTH
// a Mirror AND the authored bucket config, the namespace is active, and the
// server KV backing stream is a live mirror with replicated keys. Before the fix
// the effective spec kept the mirror, shouldConvertActiveRole never fired, and
// the scope KV stayed a server-side mirror with no primary — exactly the prod
// failover gap on pre-approval-risk-bucket / quotefeed-bucket. With the
// active-role translation the mirror is stripped → primary-form effective spec →
// in-place UpdateKeyValue promote converges the server mirror to a primary, keys
// preserved, same backing stream (no delete).
//
// Same fixture-fidelity note as Test_keyValuePromoteInPlace_preservesKeys: the
// peer KV's subjects are transformed to the dst bucket's "$KV.<dstBucket>.>" so
// the replicated keys land under the dst bucket's own subjects (the shape a
// same-name cross-region mirror yields), making them readable after promote.
func Test_activeRoleTranslation_reconcilePromotesMirrorBaselineKV(t *testing.T) {
	srv, mgr, _, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		natsNS       = "nats"
		sourceBucket = "quotefeed-bucket-peer"
		dstBucket    = "quotefeed-bucket"
		nKeys        = 11
	)
	ctx := context.Background()

	// Build the KV data plane via the new jetstream API on a fresh conn.
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	jsNew, err := jetstream.New(nc)
	require.NoError(t, err)

	// Peer KV bucket with N keys (the data to preserve).
	source, err := jsNew.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sourceBucket})
	require.NoError(t, err)
	want := make(map[string]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("quote.%d", i)
		v := fmt.Sprintf("px-%d", i)
		_, perr := source.Put(ctx, k, []byte(v))
		require.NoError(t, perr)
		want[k] = v
	}

	// MIRROR KV bucket "quotefeed-bucket" replicating the peer, subjects
	// transformed to its own "$KV.quotefeed-bucket.>".
	_, err = jsNew.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: dstBucket,
		Mirror: &jetstream.StreamSource{
			Name: kvStreamPrefix + sourceBucket,
			SubjectTransforms: []jetstream.SubjectTransformConfig{
				{Source: fmt.Sprintf("$KV.%s.>", sourceBucket), Destination: fmt.Sprintf("$KV.%s.>", dstBucket)},
			},
		},
	})
	require.NoError(t, err)

	dstStreamName := kvStreamPrefix + dstBucket
	require.Eventually(t, func() bool {
		s, e := jsNew.Stream(ctx, dstStreamName)
		if e != nil {
			return false
		}
		info, ie := s.Info(ctx)
		return ie == nil && info.State.Msgs == nKeys
	}, 5*time.Second, 100*time.Millisecond, "mirror KV must replicate all keys before the flip")

	beforeS, err := jsNew.Stream(ctx, dstStreamName)
	require.NoError(t, err)
	beforeInfo, err := beforeS.Info(ctx)
	require.NoError(t, err)
	createdBefore := beforeInfo.Created
	require.NotNil(t, beforeInfo.Config.Mirror, "precondition: dst KV backing stream is a mirror before reconcile")

	// Source releases first (DemotingSource ran) so the promote avoids 10065.
	require.NoError(t, mgr.DeleteStream(kvStreamPrefix+sourceBucket))

	// K8s side: active namespace + scope-labeled MIRROR-BASELINE KV CR — it
	// carries BOTH a Mirror AND the authored bucket config, exactly the gitops
	// form that stranded the prod flip. (The companion stream test
	// Test_activeRoleTranslation_reconcileFlipsKeyValueServerMirrorToPrimaryInPlace
	// already covers the primary-form CR; this one exercises the new
	// active-translation strip branch, which only fires when the CR has a mirror.)
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	kvCR := &api.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "quotefeed-bucket-dev",
			Namespace:  natsNS,
			Generation: 1,
			Labels:     map[string]string{scopeLabel: "true"},
		},
		Spec: api.KeyValueSpec{
			Bucket: dstBucket,
			Mirror: &api.StreamSource{Name: dstStreamName}, // mirror-baseline form
		},
		Status: api.Status{
			Conditions: []api.Condition{{Type: readyCondType, Status: corev1.ConditionUnknown}},
		},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(ns, kvCR).
		WithStatusSubresource(&api.KeyValue{}).
		Build()

	// Real production controller. EnablePassiveRoleTranslation:true is required
	// for the active-translation gate (shouldTranslateActiveRole) to fire.
	// MirrorRecreateOnConflict:false proves the active-promote path is what
	// fires, not the destructive flip path.
	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                    natsNS,
		MirrorRecreateOnConflict:     false,
		EnablePassiveRoleTranslation: true,
		CrossRegionNATSDomain:        "peer",
	})
	require.NoError(t, err)
	r := &KeyValueReconciler{Scheme: sch, JetStreamController: base}

	// DRIVE THE REAL KV RECONCILE ENTRY POINT — must strip the CR mirror and
	// drive the in-place promote to success.
	require.NoError(t, r.createOrUpdate(ctx, logr.Discard(), kvCR),
		"createOrUpdate must strip the CR mirror and drive the in-place KV promote to success")

	// All keys retained + readable; backing stream is the SAME stream, now primary.
	promoted, err := jsNew.KeyValue(ctx, dstBucket)
	require.NoError(t, err)
	keys, err := promoted.Keys(ctx)
	require.NoError(t, err)
	assert.Len(t, keys, nKeys, "all keys must survive the reconcile-driven in-place KV promote")
	for k, v := range want {
		entry, gerr := promoted.Get(ctx, k)
		require.NoError(t, gerr, "key %q must still exist after the reconcile", k)
		assert.Equal(t, v, string(entry.Value()), "value for key %q must be preserved", k)
	}
	afterS, err := jsNew.Stream(ctx, dstStreamName)
	require.NoError(t, err)
	afterInfo, err := afterS.Info(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, nKeys, afterInfo.State.Msgs, "all keys must remain on the backing stream")
	assert.Nil(t, afterInfo.Config.Mirror, "dst KV must be PRIMARY-form (Mirror==nil) after the reconcile")
	assert.Equal(t, createdBefore, afterInfo.Created, "KV backing stream must be the SAME stream (Created unchanged) — reconcile must NOT delete+recreate")

	// CR stays mirror-baseline form (server-side only translation).
	gotCR := &api.KeyValue{}
	require.NoError(t, k8s.Get(ctx, types.NamespacedName{Namespace: natsNS, Name: "quotefeed-bucket-dev"}, gotCR))
	assert.NotNil(t, gotCR.Spec.Mirror, "KV CR must stay as authored (active-translation is server-side only)")
}

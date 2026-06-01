package controller

import (
	"context"
	"encoding/json"
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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// Test_convergedSkip_doesNotSwallowMirrorToPrimaryPromote reproduces the PRODUCTION silent
// no-op: a scope-labeled, primary-form CR whose SERVER stream is a mirror, with
// ObservedGeneration == Generation AND a stored-state annotation equal to the
// (mirror) server state. The converged-skip check at stream_controller.go:364
// (`diff == "" → return nil`) short-circuits BEFORE UpdateConfiguration, so the
// activePromote/Mirror→primary-promote-detected logs fire every reconcile but
// the mirror is NEVER dropped — exactly the live loop observed on dev.
//
// This MUST FAIL against the current code (mirror survives) and PASS after the fix.
func Test_convergedSkip_doesNotSwallowMirrorToPrimaryPromote(t *testing.T) {
	srv, mgr, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		nMsgs      = 13
		streamName = "DSTCONV"
		natsNS     = "nats"
	)

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINCONV", Subjects: []string{"conv.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, perr := js.Publish("conv.evt", []byte("payload"))
		require.NoError(t, perr)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Mirror: &nats.StreamSource{Name: "ORIGINCONV"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo(streamName)
		return e == nil && info.State.Msgs == nMsgs
	}, 5*time.Second, 100*time.Millisecond)

	// Capture the server (mirror) config to seed the stored-state annotation —
	// this is what a prior passive-era reconcile would have persisted.
	dst, err := mgr.LoadStream(streamName)
	require.NoError(t, err)
	mirrorCfg := dst.Configuration()
	storedJSON, err := json.Marshal(mirrorCfg)
	require.NoError(t, err)

	// Source releases its subjects (DemotingSource ran) so the promote can't be
	// blamed on a 10065 overlap — isolate the converged-skip behavior.
	require.NoError(t, mgr.DeleteStream("ORIGINCONV"))

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	streamCR := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "dstconv-dev",
			Namespace:   natsNS,
			Generation:  1,
			Labels:      map[string]string{scopeLabel: "true"},
			Annotations: map[string]string{stateAnnotationStream: string(storedJSON)},
		},
		Spec: api.StreamSpec{
			Name:      streamName,
			Subjects:  []string{"conv.>"},
			Storage:   "file",
			Retention: "limits",
		},
		// PRODUCTION SHAPE: ObservedGeneration == Generation → the converged-skip
		// check is reached. Combined with stored==server(mirror) it returns nil.
		Status: api.Status{
			ObservedGeneration: 1,
			Conditions:         []api.Condition{{Type: readyCondType, Status: corev1.ConditionTrue}},
		},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(ns, streamCR).
		WithStatusSubresource(&api.Stream{}).
		Build()

	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                natsNS,
		MirrorRecreateOnConflict: false,
	})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: sch, JetStreamController: base}

	require.NoError(t, r.createOrUpdate(context.Background(), logr.Discard(), streamCR))

	after, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	t.Logf("AFTER reconcile: Mirror=%v Subjects=%v Msgs=%d", after.Config.Mirror != nil, after.Config.Subjects, after.State.Msgs)
	assert.Nil(t, after.Config.Mirror, "server stream MUST be primary-form after the promote reconcile (the bug leaves it a mirror)")
	assert.EqualValues(t, nMsgs, after.State.Msgs, "all messages retained")
}

// Test_keyValueConvergedSkip_doesNotSwallowMirrorToPrimaryPromote is the KeyValue analog of
// Test_convergedSkip_doesNotSwallowMirrorToPrimaryPromote: a scope-labeled, primary-form KV CR
// whose backing stream is a mirror, with ObservedGeneration==Generation AND a
// stored-state annotation equal to the (mirror) backing-stream config. The
// converged-skip in keyvalue_controller.go would short-circuit BEFORE
// UpdateKeyValue, so the backing stream stays a mirror (keys frozen).
//
// MUST FAIL before the fix, PASS after.
func Test_keyValueConvergedSkip_doesNotSwallowMirrorToPrimaryPromote(t *testing.T) {
	srv, mgr, _, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		natsNS       = "nats"
		sourceBucket = "consumer-offsets-peer"
		dstBucket    = "consumer-offsets"
		nKeys        = 9
	)
	ctx := context.Background()

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	jsNew, err := jetstream.New(nc)
	require.NoError(t, err)

	source, err := jsNew.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sourceBucket})
	require.NoError(t, err)
	want := make(map[string]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("offset.%d", i)
		v := fmt.Sprintf("value-%d", i)
		_, perr := source.Put(ctx, k, []byte(v))
		require.NoError(t, perr)
		want[k] = v
	}

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
	}, 5*time.Second, 100*time.Millisecond)

	// Capture the (mirror) backing-stream config for the stored-state annotation.
	beforeS, err := jsNew.Stream(ctx, dstStreamName)
	require.NoError(t, err)
	beforeInfo, err := beforeS.Info(ctx)
	require.NoError(t, err)
	storedJSON, err := json.Marshal(beforeInfo.Config)
	require.NoError(t, err)

	// Source releases first so the promote isn't blamed on 10065.
	require.NoError(t, mgr.DeleteStream(kvStreamPrefix+sourceBucket))

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	kvCR := &api.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "consumer-offsets-dev",
			Namespace:   natsNS,
			Generation:  1,
			Labels:      map[string]string{scopeLabel: "true"},
			Annotations: map[string]string{stateAnnotationKV: string(storedJSON)},
		},
		Spec: api.KeyValueSpec{Bucket: dstBucket},
		Status: api.Status{
			ObservedGeneration: 1,
			Conditions:         []api.Condition{{Type: readyCondType, Status: corev1.ConditionTrue}},
		},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(ns, kvCR).
		WithStatusSubresource(&api.KeyValue{}).
		Build()

	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                natsNS,
		MirrorRecreateOnConflict: false,
	})
	require.NoError(t, err)
	r := &KeyValueReconciler{Scheme: sch, JetStreamController: base}

	require.NoError(t, r.createOrUpdate(ctx, logr.Discard(), kvCR))

	afterS, err := jsNew.Stream(ctx, dstStreamName)
	require.NoError(t, err)
	afterInfo, err := afterS.Info(ctx)
	require.NoError(t, err)
	t.Logf("AFTER reconcile: Mirror=%v", afterInfo.Config.Mirror != nil)
	assert.Nil(t, afterInfo.Config.Mirror, "KV backing stream MUST be primary-form after the promote reconcile (the bug leaves it a mirror)")

	promoted, err := jsNew.KeyValue(ctx, dstBucket)
	require.NoError(t, err)
	keys, err := promoted.Keys(ctx)
	require.NoError(t, err)
	assert.Len(t, keys, nKeys, "all keys retained after the promote")
}

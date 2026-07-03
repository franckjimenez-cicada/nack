package roletranslate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nack/pkg/roletranslate"
)

// The two DRP contract strings this test stamps. roletranslate does not
// re-export these: they are the shared drp-operator<->nack annotation/label
// contract, already owned and used independently on the drp-operator side.
const (
	localRoleAnnotation = "drp.cicada.io/local-role"
	localRoleActive     = "active"
	scopeLabel          = "drp.cicada.io/nats-failover-scope"
	readyCondType       = "Ready"
)

// newJSTestServerWithURL spins up an embedded JetStream NATS server and
// returns its client URL alongside a jsm.Manager + JetStreamContext, mirroring
// internal/controller's own test helper of the same name (kept private/
// unexported to internal/controller, so this package needs its own copy —
// it drives no translation logic itself, only test-server bootstrap).
func newJSTestServerWithURL(t *testing.T) (*server.Server, *jsm.Manager, nats.JetStreamContext, func()) {
	t.Helper()
	opts := &natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	dir, err := os.MkdirTemp("", "nats-roletranslate-*")
	require.NoError(t, err)
	opts.StoreDir = dir

	srv := natsserver.RunServer(opts)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	js, err := nc.JetStream()
	require.NoError(t, err)
	mgr, err := jsm.New(nc)
	require.NoError(t, err)

	cleanup := func() {
		nc.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
	}
	return srv, mgr, js, cleanup
}

func xlatScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	require.NoError(t, scheme.AddToScheme(sch))
	require.NoError(t, api.AddToScheme(sch))
	return sch
}

// Test_StreamReconciler_CreateOrUpdate_PromotesMirrorToPrimaryInPlace proves
// the exposed seam delegates to the REAL production translation + apply
// logic: it reproduces internal/controller's own
// Test_activeRoleTranslation_reconcileFlipsServerMirrorToPrimaryInPlace, but
// driven entirely THROUGH this package's public API (roletranslate.NewController
// + roletranslate.StreamReconciler{...}.CreateOrUpdate) as an external
// consumer (e.g. drp-operator's integration harness) would call it — never
// touching internal/controller directly.
//
// Scenario: a scope-labeled, primary-form Stream CR whose server-side stream
// is still a mirror (the passive-role-translation steady state) sits in a
// namespace the drp-operator has just stamped drp.cicada.io/local-role=active
// on. CreateOrUpdate must convert the server stream to a primary IN PLACE
// (never delete+recreate), preserving every replicated message.
func Test_StreamReconciler_CreateOrUpdate_PromotesMirrorToPrimaryInPlace(t *testing.T) {
	srv, mgr, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		nMsgs      = 21
		streamName = "RTXLATDST"
		natsNS     = "nats"
	)

	// Server-side: an origin primary with messages, replicated into a mirror
	// destination stream — the passive-side steady state before the flip.
	_, err := js.AddStream(&nats.StreamConfig{Name: "RTXLATORIGIN", Subjects: []string{"rtx.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, perr := js.Publish("rtx.evt", []byte("payload"))
		require.NoError(t, perr)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Mirror: &nats.StreamSource{Name: "RTXLATORIGIN"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo(streamName)
		return e == nil && info.State.Msgs == nMsgs
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate all messages before the flip")

	beforeInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	createdBefore := beforeInfo.Created
	require.NotNil(t, beforeInfo.Config.Mirror, "precondition: server stream is a mirror before reconcile")

	// The source releases its subjects first (DemotingSource ran), so the
	// in-place promote does not hit the transient 10065 subject-overlap.
	require.NoError(t, mgr.DeleteStream("RTXLATORIGIN"))

	// K8s side (fake client): the drp-operator has stamped the `nats`
	// namespace active, and the scope-labeled Stream CR is primary-form (the
	// passive-translation steady shape — the CR itself is never mutated by
	// the operator promote).
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	streamCR := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rtxlatdst-dev",
			Namespace:  natsNS,
			Generation: 1,
			Labels:     map[string]string{scopeLabel: "true"},
		},
		Spec: api.StreamSpec{
			Name:      streamName,
			Subjects:  []string{"rtx.>"},
			Storage:   "file",
			Retention: "limits",
		},
		Status: api.Status{
			Conditions: []api.Condition{{Type: readyCondType, Status: corev1.ConditionUnknown}},
		},
	}
	sch := xlatScheme(t)
	k8s := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(ns, streamCR).
		WithStatusSubresource(&api.Stream{}).
		Build()

	// Build the REAL controller through the public seam. MirrorRecreateOnConflict
	// is false, proving the active-role in-place promote fires independently of
	// the destructive-recreate flag.
	base, err := roletranslate.NewController(k8s, &roletranslate.NatsConfig{ServerURL: srv.ClientURL()}, &roletranslate.Config{
		Namespace:                natsNS,
		MirrorRecreateOnConflict: false,
	})
	require.NoError(t, err)
	r := &roletranslate.StreamReconciler{Scheme: sch, JetStreamController: base}

	// DRIVE THE REAL TRANSLATION + APPLY LOGIC through the exported seam.
	require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), streamCR),
		"CreateOrUpdate must route the active scope mirror->primary flip in place")

	// The server stream must now be a primary with all data retained.
	afterInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	assert.EqualValues(t, nMsgs, afterInfo.State.Msgs, "ALL messages must survive the promote")
	assert.Nil(t, afterInfo.Config.Mirror, "server stream must be PRIMARY-form after CreateOrUpdate")
	assert.Equal(t, []string{"rtx.>"}, afterInfo.Config.Subjects, "server stream must own the authored subjects")
	assert.Equal(t, createdBefore, afterInfo.Created, "must be the SAME stream — never delete+recreate")

	// The K8s CR must remain primary-form + untouched (server-side only flip).
	gotCR := &api.Stream{}
	require.NoError(t, k8s.Get(context.Background(), types.NamespacedName{Namespace: natsNS, Name: "rtxlatdst-dev"}, gotCR))
	assert.Nil(t, gotCR.Spec.Mirror, "CR must stay primary-form (active-translation is server-side only)")
}

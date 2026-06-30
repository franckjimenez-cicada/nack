package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// Test_shouldTranslateActiveRole_gate pins the active-translation predicate.
func Test_shouldTranslateActiveRole_gate(t *testing.T) {
	// Fires: enabled + scope-labeled + active + CR has a mirror to strip.
	assert.True(t, shouldTranslateActiveRole(true, true, localRoleActive, false, true))
	// Absent role + NOT cold-start-passive ⇒ active default ⇒ fires.
	assert.True(t, shouldTranslateActiveRole(true, true, "", false, true))
	// Passive ns ⇒ never (passive translation owns that direction).
	assert.False(t, shouldTranslateActiveRole(true, true, localRolePassive, false, true))
	// Absent role + cold-start-default-passive ⇒ treated passive ⇒ no.
	assert.False(t, shouldTranslateActiveRole(true, true, "", true, true))
	// No mirror to strip ⇒ no (CR already primary form).
	assert.False(t, shouldTranslateActiveRole(true, true, localRoleActive, false, false))
	// Not scope-labeled / feature off ⇒ no (never touch arbitrary primaries).
	assert.False(t, shouldTranslateActiveRole(true, false, localRoleActive, false, true))
	assert.False(t, shouldTranslateActiveRole(false, true, localRoleActive, false, true))
}

// Test_translateStreamSpecToPrimary asserts the inverse translation drops the
// mirror + sources and keeps the authored subjects, without mutating the input.
func Test_translateStreamSpecToPrimary(t *testing.T) {
	orig := &api.StreamSpec{
		Name:     "S",
		Subjects: []string{"s.>"},
		Mirror:   &api.StreamSource{Name: "S"},
		Sources:  []*api.StreamSource{{Name: "OTHER"}},
	}
	out := translateStreamSpecToPrimary(orig)
	assert.Nil(t, out.Mirror, "mirror must be stripped")
	assert.Nil(t, out.Sources, "sources must be stripped")
	assert.Equal(t, []string{"s.>"}, out.Subjects, "authored subjects must be kept")
	assert.NotNil(t, orig.Mirror, "input CR spec must be untouched (deep copy)")
}

// Test_activeRoleTranslation_reconcilePromotesMirrorBaselineCR is the prod
// scenario: the gitops "mirror baseline" CR carries BOTH a Mirror AND the
// authored Subjects, the namespace is active, and the server stream is a live
// mirror with replicated messages. Before the fix the effective spec kept the
// mirror, shouldConvertActiveRole never fired, and nack applied a mirror+subjects
// config the server rejects (10034). With the active-role translation, the
// mirror is stripped → primary-form effective spec → two-phase in-place promote
// converges the server mirror to a primary, messages preserved, no delete.
func Test_activeRoleTranslation_reconcilePromotesMirrorBaselineCR(t *testing.T) {
	srv, mgr, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		nMsgs      = 17
		streamName = "DSTMB"
		natsNS     = "nats"
	)

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINMB", Subjects: []string{"mb.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, perr := js.Publish("mb.evt", []byte("payload"))
		require.NoError(t, perr)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Mirror: &nats.StreamSource{Name: "ORIGINMB"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo(streamName)
		return e == nil && info.State.Msgs == nMsgs
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate before the flip")

	beforeInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	createdBefore := beforeInfo.Created
	require.NotNil(t, beforeInfo.Config.Mirror, "precondition: server stream is a mirror")

	// DemotingSource already released the subjects (avoids 10065).
	require.NoError(t, mgr.DeleteStream("ORIGINMB"))

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	// MIRROR-BASELINE CR: carries BOTH a Mirror and the authored Subjects —
	// exactly the gitops form that stranded the prod flip.
	streamCR := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "dstmb-dev",
			Namespace:  natsNS,
			Generation: 1,
			Labels:     map[string]string{scopeLabel: "true"},
		},
		Spec: api.StreamSpec{
			Name:      streamName,
			Subjects:  []string{"mb.>"},
			Mirror:    &api.StreamSource{Name: streamName},
			Storage:   "file",
			Retention: "limits",
		},
		Status: api.Status{
			Conditions: []api.Condition{{Type: readyCondType, Status: corev1.ConditionUnknown}},
		},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(ns, streamCR).
		WithStatusSubresource(&api.Stream{}).
		Build()

	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                    natsNS,
		MirrorRecreateOnConflict:     false,
		EnablePassiveRoleTranslation: true,
		CrossRegionNATSDomain:        "peer",
	})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: sch, JetStreamController: base}

	require.NoError(t, r.createOrUpdate(context.Background(), logr.Discard(), streamCR),
		"createOrUpdate must strip the CR mirror and drive the in-place promote to success")

	afterInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	assert.EqualValues(t, nMsgs, afterInfo.State.Msgs, "ALL messages must survive")
	assert.Nil(t, afterInfo.Config.Mirror, "server stream must be PRIMARY-form after the reconcile")
	assert.Equal(t, []string{"mb.>"}, afterInfo.Config.Subjects, "server stream must own the authored subjects")
	assert.Equal(t, createdBefore, afterInfo.Created, "must be the SAME stream — never delete+recreate")
}

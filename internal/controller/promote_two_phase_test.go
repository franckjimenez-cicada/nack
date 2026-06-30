/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	jsmapi "github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nats-io/jsm.go"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// Test_targetConfigHasSubjects pins the phase-split gate: the two-phase
// promote must fire ONLY when the primary target carries subjects (the shape
// that trips 10034 on a still-mirror stream). A subjectless target converges
// in a single update and must keep the normal single-update path.
func Test_targetConfigHasSubjects(t *testing.T) {
	assert.True(t, targetConfigHasSubjects(&api.StreamSpec{Subjects: []string{"a.>"}}),
		"primary spec with subjects must take the two-phase split")
	assert.False(t, targetConfigHasSubjects(&api.StreamSpec{}),
		"subjectless primary already converges in one update — no split")
	assert.False(t, targetConfigHasSubjects(&api.StreamSpec{Subjects: []string{}}),
		"empty subjects slice must not trigger the split")
	assert.False(t, targetConfigHasSubjects(nil))
}

// Test_unmirrorStreamOpts_clearsMirrorAndSubjects asserts the phase-1
// ("un-mirror") option set, applied on top of a live mirror-bearing server
// config, drops Mirror + Sources AND leaves Subjects empty. A standalone
// stream with no subjects is valid and does NOT trip 10034 — that is the whole
// point of phase 1. (Phase 2 then adds the authored subjects.)
func Test_unmirrorStreamOpts_clearsMirrorAndSubjects(t *testing.T) {
	// Live server config: a mirror that has somehow accreted sources too.
	serverCfg := jsmapi.StreamConfig{
		Name:    "S",
		Mirror:  &jsmapi.StreamSource{Name: "ORIGIN"},
		Sources: []*jsmapi.StreamSource{{Name: "OTHER"}},
	}

	// Build the phase-1 config the same way UpdateConfiguration does — apply the
	// opts on top of the live server config (its base).
	cfg, err := jsm.NewStreamConfiguration(serverCfg, unmirrorStreamOpts()...)
	require.NoError(t, err)

	assert.Nil(t, cfg.Mirror, "phase-1 must clear the server-side mirror")
	assert.Empty(t, cfg.Sources, "phase-1 must clear server-side sources")
	assert.Empty(t, cfg.Subjects, "phase-1 must NOT set subjects — a subjectless standalone stream avoids 10034")
}

// Test_streamPromoteInPlace_twoPhaseSequencePreservesMessages drives the EXACT
// two-phase sequence the reconciler now performs against the real embedded
// nats-server (v2.14.0, pinned by go.mod): phase 1 un-mirror (no subjects),
// reload, phase 2 add the authored subjects. It proves each phase is
// individually accepted by nats-server, the stream is NEVER deleted, and ALL
// replicated messages survive.
//
// REAL-NATS CAVEAT: the pinned embedded server ACCEPTS the one-shot
// subjects-on-mirror update, so it cannot reproduce the PROD 10034 rejection —
// this test therefore proves the two-phase sequence is itself data-safe and
// server-accepted, not that the old single update would have failed here. The
// 10034 rejection is server-version specific; reproducing it needs the prod
// server version (see the e2e/kind path note in the PR).
func Test_streamPromoteInPlace_twoPhaseSequencePreservesMessages(t *testing.T) {
	mgr, js, cleanup := newJSTestServer(t)
	defer cleanup()

	const nMsgs = 21

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"act.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, perr := js.Publish("act.evt", []byte("payload"))
		require.NoError(t, perr)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: "DST", Mirror: &nats.StreamSource{Name: "ORIGIN"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo("DST")
		return e == nil && info.State.Msgs == nMsgs
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate all messages before promote")

	beforeInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	createdBefore := beforeInfo.Created

	// Source releases subjects first (DemotingSource ran) so neither phase hits 10065.
	require.NoError(t, mgr.DeleteStream("ORIGIN"))

	// PHASE 1 — un-mirror with NO subjects.
	dst, err := mgr.LoadStream("DST")
	require.NoError(t, err)
	serverCfg := dst.Configuration()
	require.NotNil(t, serverCfg.Mirror, "precondition: DST is a mirror before phase 1")
	require.NoError(t, dst.UpdateConfiguration(serverCfg, unmirrorStreamOpts()...),
		"phase-1 un-mirror (no subjects) must be accepted by nats-server")

	// After phase 1: the load-bearing change is that the stream is no longer a
	// MIRROR. (nats-server defaults a subjectless standalone stream's subject to
	// the stream name; that is fine — what matters is that the stream is now
	// standalone, so phase 2 can set the authored subjects without 10034.) Data
	// must be intact and the stream must be the same one (never recreated).
	midInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	assert.Nil(t, midInfo.Config.Mirror, "phase 1 must drop the mirror — this is what unblocks the subjects update in phase 2")
	assert.EqualValues(t, nMsgs, midInfo.State.Msgs, "phase 1 must RETAIN all replicated messages (no delete)")
	assert.Equal(t, createdBefore, midInfo.Created, "phase 1 must not recreate the stream")

	// PHASE 2 — reload fresh (now-standalone) state, add authored subjects.
	s2, err := mgr.LoadStream("DST")
	require.NoError(t, err)
	freshCfg := s2.Configuration()

	// Idempotency: a now-standalone stream is no longer a mirror→primary flip,
	// so a re-entrant reconcile would SKIP the split and just do the single
	// add-subjects update — proving convergence across passes.
	assert.False(t, streamMirrorToPrimaryFlip(&freshCfg, &api.StreamSpec{Name: "DST", Subjects: []string{"act.>"}}),
		"after phase 1 the stream is standalone — the split must not re-fire on the next reconcile")

	spec := &api.StreamSpec{Name: "DST", Subjects: []string{"act.>"}}
	opts, err := streamSpecToConfig(spec, &freshCfg)
	require.NoError(t, err)
	require.NoError(t, s2.UpdateConfiguration(freshCfg, opts...),
		"phase-2 add-subjects on the now-standalone stream must be accepted by nats-server")

	// Final: primary-form, subjects set, data intact, same stream.
	afterInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	assert.Nil(t, afterInfo.Config.Mirror, "DST must be primary-form after the two-phase promote")
	assert.Equal(t, []string{"act.>"}, afterInfo.Config.Subjects, "DST must own the authored subjects after phase 2")
	assert.EqualValues(t, nMsgs, afterInfo.State.Msgs, "ALL messages must survive the two-phase promote")
	assert.Equal(t, createdBefore, afterInfo.Created, "server stream must be the SAME stream (never delete+recreated)")
	assert.EqualValues(t, 1, afterInfo.State.FirstSeq, "first sequence preserved — not a fresh seq-0 epoch")
}

// Test_twoPhasePromote_reconcileFlipsServerMirrorToPrimaryInPlace drives the
// REAL production reconcile entry point — StreamReconciler.createOrUpdate —
// against an embedded nats-server with a real jsController and a fake k8s
// client, exercising the NEW two-phase code path end to end. It proves the
// reconcile routes a scope-labeled + active + server-mirror + subjects CR
// through the in-place promote (now two-phase internally), preserves all
// messages, leaves the CR primary-form, and NEVER deletes the stream.
// MirrorRecreateOnConflict is false — proving the active-promote wiring, not
// the destructive flip path, is what fires.
func Test_twoPhasePromote_reconcileFlipsServerMirrorToPrimaryInPlace(t *testing.T) {
	srv, mgr, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		nMsgs      = 23
		streamName = "DST2PH"
		natsNS     = "nats"
	)

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN2PH", Subjects: []string{"tp.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, perr := js.Publish("tp.evt", []byte("payload"))
		require.NoError(t, perr)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Mirror: &nats.StreamSource{Name: "ORIGIN2PH"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo(streamName)
		return e == nil && info.State.Msgs == nMsgs
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate all messages before the flip")

	beforeInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	createdBefore := beforeInfo.Created
	require.NotNil(t, beforeInfo.Config.Mirror, "precondition: server stream is a mirror before reconcile")

	// Source releases subjects first (DemotingSource ran) so the promote avoids 10065.
	require.NoError(t, mgr.DeleteStream("ORIGIN2PH"))

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	streamCR := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "dst2ph-dev",
			Namespace:  natsNS,
			Generation: 1,
			Labels:     map[string]string{scopeLabel: "true"},
		},
		Spec: api.StreamSpec{
			Name:      streamName,
			Subjects:  []string{"tp.>"},
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
		Namespace:                natsNS,
		MirrorRecreateOnConflict: false,
	})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: sch, JetStreamController: base}

	require.NoError(t, r.createOrUpdate(context.Background(), logr.Discard(), streamCR),
		"createOrUpdate must drive the two-phase in-place mirror→primary promote to success")

	afterInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	assert.EqualValues(t, nMsgs, afterInfo.State.Msgs, "ALL messages must survive the reconcile-driven two-phase promote")
	assert.Nil(t, afterInfo.Config.Mirror, "server stream must be PRIMARY-form after the reconcile")
	assert.Equal(t, []string{"tp.>"}, afterInfo.Config.Subjects, "server stream must own the authored subjects after the reconcile")
	assert.Equal(t, createdBefore, afterInfo.Created, "server stream must be the SAME stream — the reconcile must NOT delete+recreate")
	assert.EqualValues(t, 1, afterInfo.State.FirstSeq, "first sequence preserved — not a fresh seq-0 epoch")
}

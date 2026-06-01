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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jsm.go"
	jsmapi "github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// newJSTestServerWithURL spins up an embedded JetStream NATS server and
// returns its client URL alongside a jsm.Manager + JetStreamContext, so a
// test can ALSO build a production jsController pointed at the SAME server.
// Same v2.14.0 pin as newJSTestServer.
func newJSTestServerWithURL(t *testing.T) (*server.Server, *jsm.Manager, nats.JetStreamContext, func()) {
	t.Helper()
	opts := &natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	dir, err := os.MkdirTemp("", "nats-active-xlat-*")
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

// newJSTestServer spins up an embedded JetStream NATS server (same pattern as
// TestStreamUpdateWithoutPlacement) and returns a connected jsm.Manager + a
// raw JetStreamContext for publishing. nats-server version is pinned by go.mod
// (currently v2.14.0) so these assertions reflect the EXACT server the dev
// clusters run.
func newJSTestServer(t *testing.T) (*jsm.Manager, nats.JetStreamContext, func()) {
	t.Helper()
	opts := &natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	dir, err := os.MkdirTemp("", "nats-promote-*")
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
	return mgr, js, cleanup
}

// Test_streamMirrorToPrimaryFlip asserts the direction predicate that gates the
// data-preserving in-place path: it fires ONLY for server-mirror + spec-primary.
func Test_streamMirrorToPrimaryFlip(t *testing.T) {
	mirrorCfg := &jsmapi.StreamConfig{Mirror: &jsmapi.StreamSource{Name: "X"}}
	primaryCfg := &jsmapi.StreamConfig{Subjects: []string{"a.>"}}
	mirrorSpec := &api.StreamSpec{Mirror: &api.StreamSource{Name: "X"}}
	primarySpec := &api.StreamSpec{Subjects: []string{"a.>"}}

	assert.True(t, streamMirrorToPrimaryFlip(mirrorCfg, primarySpec), "server mirror + spec primary = promote")
	assert.False(t, streamMirrorToPrimaryFlip(primaryCfg, mirrorSpec), "server primary + spec mirror = demote, NOT a promote")
	assert.False(t, streamMirrorToPrimaryFlip(mirrorCfg, mirrorSpec), "both mirror = no flip")
	assert.False(t, streamMirrorToPrimaryFlip(primaryCfg, primarySpec), "both primary = no flip")
	assert.False(t, streamMirrorToPrimaryFlip(nil, primarySpec))
}

// Test_streamSpecToConfig_clearsMirrorWhenSpecPrimary asserts the load-bearing
// targetConfig change: when the spec carries no mirror, streamSpecToConfig emits
// an opt that explicitly clears o.Mirror (and o.Sources). Without this, the
// in-place UpdateConfiguration — which uses the live (mirror-bearing) server
// config as its base — would leave the mirror set, turning the promote into a
// no-op flip that forces the destructive delete+recreate path.
func Test_streamSpecToConfig_clearsMirrorWhenSpecPrimary(t *testing.T) {
	// Server currently a mirror; spec is a primary (mirror dropped).
	serverCfg := &jsmapi.StreamConfig{
		Name:    "S",
		Mirror:  &jsmapi.StreamSource{Name: "ORIGIN"},
		Sources: []*jsmapi.StreamSource{{Name: "OTHER"}},
	}
	spec := &api.StreamSpec{Name: "S", Subjects: []string{"promoted.>"}}

	opts, err := streamSpecToConfig(spec, serverCfg)
	require.NoError(t, err)

	// Apply opts on top of the server config (the in-place update base).
	cfg, err := jsm.NewStreamConfiguration(*serverCfg, opts...)
	require.NoError(t, err)

	assert.Nil(t, cfg.Mirror, "promote targetConfig must CLEAR the server-side mirror")
	assert.Empty(t, cfg.Sources, "promote targetConfig must clear server-side sources")
	assert.Equal(t, []string{"promoted.>"}, cfg.Subjects, "promote targetConfig must set the authored subjects")
}

// Test_shouldConvertActiveRole_gating pins the ACTIVE-role-translation
// predicate: it fires ONLY for a scope-labeled, primary-form CR whose server
// stream is currently a mirror, on an ACTIVE (or role-absent) namespace.
func Test_shouldConvertActiveRole_gating(t *testing.T) {
	// Happy: scope-labeled, active (""=active default), spec primary, server mirror.
	assert.True(t, shouldConvertActiveRole(true, "", false, true),
		"scope + active(default) + spec-primary + server-mirror must convert")
	assert.True(t, shouldConvertActiveRole(true, localRoleActive, false, true),
		"explicit active must convert")

	// Negatives — each gate independently blocks.
	assert.False(t, shouldConvertActiveRole(false, localRoleActive, false, true),
		"NON-scope-labeled primary must NEVER be touched (steady-state primary safety)")
	assert.False(t, shouldConvertActiveRole(true, localRolePassive, false, true),
		"passive role is the passive path's job — active-translation must not fire")
	assert.False(t, shouldConvertActiveRole(true, localRoleActive, true, true),
		"spec is mirror form — active-translation never creates a mirror")
	assert.False(t, shouldConvertActiveRole(true, localRoleActive, false, false),
		"server already primary — nothing to convert (steady state)")
}

// Test_activeRoleTranslation_convertsMirrorToPrimaryInPlace is the
// ACTIVE-role-translation analog of Test_streamPromoteInPlace_preservesMessages,
// driven against the real embedded nats-server (v2.14.0, pinned by go.mod).
//
// It reproduces the failed-promote scenario the fork now fixes: under passive-
// role-translation the scope CR is ALWAYS primary-form, so when the namespace
// flips to local-role=active the reconciler must (a) recognize via
// shouldConvertActiveRole that the SERVER stream is still a mirror, and (b)
// convert it to a PRIMARY IN PLACE using the SAME PR #11 mechanism
// (streamSpecToConfig emitting an explicit Mirror=nil + UpdateConfiguration on
// the live serverState). The conversion must RETAIN every replicated message
// and set the authored subjects.
func Test_activeRoleTranslation_convertsMirrorToPrimaryInPlace(t *testing.T) {
	mgr, js, cleanup := newJSTestServer(t)
	defer cleanup()

	const nMsgs = 17

	// Origin primary with messages, replicated into a mirror DST — the
	// passive-side steady state before the active flip.
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
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate all messages before the active flip")

	beforeInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	createdBefore := beforeInfo.Created

	// GATE: simulate the active-role decision the reconciler makes. The CR is
	// scope-labeled + primary-form; the namespace is active; the server is a
	// mirror. shouldConvertActiveRole MUST say "convert".
	serverIsMirror := beforeInfo.Config.Mirror != nil
	require.True(t, serverIsMirror, "precondition: DST is a server-side mirror before the flip")
	specHasMirror := false // primary-form scope CR (passive-translation steady state)
	require.True(t,
		shouldConvertActiveRole(true /*scope-labeled*/, localRoleActive, specHasMirror, serverIsMirror),
		"active-translation gate must fire for this scope mirror under active role")

	// Source releases its subjects first (DemotingSource ran), so the in-place
	// promote does not hit 10065. (The 10065 retryable path is covered by
	// Test_streamPromoteInPlace_subjectOverlapIsRetryableNotDestructive.)
	require.NoError(t, mgr.DeleteStream("ORIGIN"))

	// CONVERT IN PLACE — the exact code path the active gate routes into:
	// streamSpecToConfig on the primary spec (emits explicit Mirror=nil),
	// then UpdateConfiguration on the live server (mirror) state.
	dst, err := mgr.LoadStream("DST")
	require.NoError(t, err)
	serverCfg := dst.Configuration()
	spec := &api.StreamSpec{Name: "DST", Subjects: []string{"act.>"}}
	opts, err := streamSpecToConfig(spec, &serverCfg)
	require.NoError(t, err)
	require.NoError(t, dst.UpdateConfiguration(serverCfg, opts...),
		"active-role in-place mirror→primary conversion must be accepted by nats-server")

	// Assertions: data retained, stream identity preserved, now primary.
	afterInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	assert.EqualValues(t, nMsgs, afterInfo.State.Msgs, "ALL messages must survive the in-place active-role conversion")
	assert.Nil(t, afterInfo.Config.Mirror, "DST must be PRIMARY-form (Mirror==nil) after active-role translation")
	assert.Equal(t, []string{"act.>"}, afterInfo.Config.Subjects, "DST must own the authored subjects after conversion")
	assert.Equal(t, createdBefore, afterInfo.Created, "server stream must be the SAME stream (Created unchanged) — never delete+recreated")
	assert.EqualValues(t, 1, afterInfo.State.FirstSeq, "first sequence preserved — not a fresh seq-0 epoch")
}

// activeXlatScheme builds a scheme with corev1 (Namespace) + the nack API
// (Stream/KeyValue) registered, for the fake controller-runtime client the
// end-to-end reconcile tests use.
func activeXlatScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	require.NoError(t, scheme.AddToScheme(sch))
	require.NoError(t, api.AddToScheme(sch))
	return sch
}

// Test_activeRoleTranslation_reconcileFlipsServerMirrorToPrimaryInPlace is the
// END-TO-END proof requested in the #12 review (the #58 hollow-test lesson):
// it drives the REAL production reconcile entry point —
// StreamReconciler.createOrUpdate — against an embedded nats-server v2.14.0,
// with a real jsController (built via NewJSController) talking to that server
// and a fake k8s client holding the namespace + CR. It proves the NEW call-site
// wiring (the activePromote branch in stream_controller.go) actually ROUTES a
// scope-labeled + local-role=active + server-mirror CR into the in-place
// promote — NOT just that the gate predicate + mechanism work in isolation.
//
// Crucially MirrorRecreateOnConflict is FALSE: that proves active-promote fires
// INDEPENDENTLY of the destructive-recreate flag (the pre-existing flip path is
// gated on that flag, so with it off, ONLY the new active-translation wiring can
// flip the server stream to primary).
func Test_activeRoleTranslation_reconcileFlipsServerMirrorToPrimaryInPlace(t *testing.T) {
	srv, mgr, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		nMsgs      = 19
		streamName = "DSTRECON"
		natsNS     = "nats"
	)

	// Server-side: ORIGIN primary with messages, replicated into mirror
	// DSTRECON — the passive-side steady state before the active flip.
	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINRECON", Subjects: []string{"actr.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, perr := js.Publish("actr.evt", []byte("payload"))
		require.NoError(t, perr)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Mirror: &nats.StreamSource{Name: "ORIGINRECON"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo(streamName)
		return e == nil && info.State.Msgs == nMsgs
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate all messages before the flip")

	beforeInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	createdBefore := beforeInfo.Created
	require.NotNil(t, beforeInfo.Config.Mirror, "precondition: server stream is a mirror before reconcile")

	// Source releases its subjects first (DemotingSource ran), so the in-place
	// promote does not hit 10065.
	require.NoError(t, mgr.DeleteStream("ORIGINRECON"))

	// K8s side (fake client): the `nats` namespace is ACTIVE, and the scope-
	// labeled Stream CR is PRIMARY-form (this is the passive-translation steady
	// shape — the CR is never mutated by the operator promote).
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	streamCR := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "dstrecon-dev",
			Namespace:  natsNS,
			Generation: 1,
			Labels:     map[string]string{scopeLabel: "true"},
		},
		Spec: api.StreamSpec{
			Name:      streamName, // server stream name the reconciler probes
			Subjects:  []string{"actr.>"},
			Storage:   "file",
			Retention: "limits",
		},
		// Ready condition pre-set so createOrUpdate runs the update branch
		// (and ObservedGeneration unset so the converged-skip short-circuit
		// at the diff check does NOT fire before the flip block).
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

	// Real production controller pointed at the SAME embedded server. Note
	// MirrorRecreateOnConflict:false — proves the active-promote path is what
	// fires, not the destructive flip path. Passive translation is irrelevant
	// here (role is active), so EnablePassiveRoleTranslation can stay false.
	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                natsNS,
		MirrorRecreateOnConflict: false,
	})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: sch, JetStreamController: base}

	// DRIVE THE REAL RECONCILE ENTRY POINT.
	require.NoError(t, r.createOrUpdate(context.Background(), logr.Discard(), streamCR),
		"createOrUpdate must succeed and route the active scope mirror→primary flip in place")

	// Assert the SERVER stream is now a PRIMARY with all data retained.
	afterInfo, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	assert.EqualValues(t, nMsgs, afterInfo.State.Msgs, "ALL messages must survive the reconcile-driven in-place active-role conversion")
	assert.Nil(t, afterInfo.Config.Mirror, "server stream must be PRIMARY-form (Mirror==nil) after the reconcile")
	assert.Equal(t, []string{"actr.>"}, afterInfo.Config.Subjects, "server stream must own the authored subjects after the reconcile")
	assert.Equal(t, createdBefore, afterInfo.Created, "server stream must be the SAME stream (Created unchanged) — the reconcile must NOT delete+recreate")
	assert.EqualValues(t, 1, afterInfo.State.FirstSeq, "first sequence preserved — not a fresh seq-0 epoch")

	// The K8s CR must remain primary-form + untouched (server-side only flip).
	gotCR := &api.Stream{}
	require.NoError(t, k8s.Get(context.Background(), types.NamespacedName{Namespace: natsNS, Name: "dstrecon-dev"}, gotCR))
	assert.Nil(t, gotCR.Spec.Mirror, "CR must stay primary-form (active-translation is server-side only)")
}

// Test_activeRoleTranslation_reconcileFlipsKeyValueServerMirrorToPrimaryInPlace
// is the KeyValue analog of the end-to-end reconcile test: it drives the REAL
// KeyValueReconciler.createOrUpdate against embedded nats-server v2.14.0 and
// proves the active-translation wiring in keyvalue_controller.go routes a
// scope-labeled + active + server-mirror KV CR into the in-place UpdateKeyValue
// promote (preserving all keys), with MirrorRecreateOnConflict=false.
//
// Same fixture fidelity note as Test_keyValuePromoteInPlace_preservesKeys: the
// peer KV's subjects are transformed to the dst bucket's "$KV.<dstBucket>.>" so
// the replicated keys land under the dst bucket's own subjects (the shape a
// same-name cross-region mirror yields), making them readable after promote.
func Test_activeRoleTranslation_reconcileFlipsKeyValueServerMirrorToPrimaryInPlace(t *testing.T) {
	srv, mgr, _, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	const (
		natsNS       = "nats"
		sourceBucket = "consumer-offsets-peer"
		dstBucket    = "consumer-offsets"
		nKeys        = 8
	)
	ctx := context.Background()

	// Build the KV data plane via the new jetstream API on a fresh conn.
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	jsNew, err := jetstream.New(nc)
	require.NoError(t, err)

	// Peer KV bucket with N keys.
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

	// MIRROR KV bucket "consumer-offsets" replicating the peer, subjects
	// transformed to its own "$KV.consumer-offsets.>".
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

	// K8s side: active namespace + scope-labeled primary-form KV CR.
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        natsNS,
			Annotations: map[string]string{localRoleAnnotation: localRoleActive},
		},
	}
	kvCR := &api.KeyValue{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "consumer-offsets-dev",
			Namespace:  natsNS,
			Generation: 1,
			Labels:     map[string]string{scopeLabel: "true"},
		},
		Spec: api.KeyValueSpec{Bucket: dstBucket}, // primary form: no Mirror.
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

	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                natsNS,
		MirrorRecreateOnConflict: false, // proves active-promote fires WITHOUT it
	})
	require.NoError(t, err)
	r := &KeyValueReconciler{Scheme: sch, JetStreamController: base}

	// DRIVE THE REAL KV RECONCILE ENTRY POINT.
	require.NoError(t, r.createOrUpdate(ctx, logr.Discard(), kvCR),
		"KeyValueReconciler.createOrUpdate must route the active scope KV mirror→primary flip in place")

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
	assert.Equal(t, createdBefore, afterInfo.Created, "KV backing stream must be the SAME stream (Created unchanged) — reconcile must NOT delete+recreate")
	assert.Nil(t, afterInfo.Config.Mirror, "dst KV must be PRIMARY-form (Mirror==nil) after the reconcile")

	// CR stays primary-form (server-side only flip).
	gotCR := &api.KeyValue{}
	require.NoError(t, k8s.Get(ctx, types.NamespacedName{Namespace: natsNS, Name: "consumer-offsets-dev"}, gotCR))
	assert.Nil(t, gotCR.Spec.Mirror, "KV CR must stay primary-form (active-translation is server-side only)")
}

// Test_streamPromoteInPlace_preservesMessages drives the REAL data-plane path:
// it builds an origin stream with messages, a mirror that replicates them, then
// performs the exact in-place conversion the reconciler now performs
// (UpdateConfiguration with the streamSpecToConfig-built mirror-clearing opts)
// and asserts the server stream is NEVER deleted and ALL messages survive.
//
// This is the proof that the fix preserves data on the real nats-server
// version — the inverse of the bug, where the promote delete-recreated the
// stream to seq 0 (empty).
func Test_streamPromoteInPlace_preservesMessages(t *testing.T) {
	mgr, js, cleanup := newJSTestServer(t)
	defer cleanup()

	// Origin primary with 25 messages.
	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"act.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		_, err := js.Publish("act.evt", []byte("payload"))
		require.NoError(t, err)
	}

	// Mirror that replicates the 25 messages.
	_, err = js.AddStream(&nats.StreamConfig{Name: "DST", Mirror: &nats.StreamSource{Name: "ORIGIN"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo("DST")
		return e == nil && info.State.Msgs == 25
	}, 5*time.Second, 100*time.Millisecond, "mirror must replicate all 25 messages before promote")

	// Capture the server stream's identity so we can prove it was NEVER deleted.
	beforeInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	createdBefore := beforeInfo.Created

	// Source releases the subjects first (mirror→primary demote), exactly as the
	// DRP DemotingSource subphase does before PromotingDestination. Without this,
	// the in-place promote hits 10065 (covered by the overlap test below).
	require.NoError(t, mgr.DeleteStream("ORIGIN"))

	// THE PROMOTE — in place, via the same code path the reconciler uses.
	dst, err := mgr.LoadStream("DST")
	require.NoError(t, err)
	serverCfg := dst.Configuration()
	require.NotNil(t, serverCfg.Mirror, "precondition: DST is a mirror before promote")

	spec := &api.StreamSpec{Name: "DST", Subjects: []string{"act.>"}}
	opts, err := streamSpecToConfig(spec, &serverCfg)
	require.NoError(t, err)
	require.NoError(t, dst.UpdateConfiguration(serverCfg, opts...), "in-place mirror→primary promote must be accepted by nats-server")

	// Assertions: stream identity preserved (NOT recreated) + all data intact.
	afterInfo, err := js.StreamInfo("DST")
	require.NoError(t, err)
	assert.EqualValues(t, 25, afterInfo.State.Msgs, "ALL 25 messages must survive the in-place promote (the data-loss bug dropped these to 0)")
	assert.Nil(t, afterInfo.Config.Mirror, "DST must be primary-form after promote")
	assert.Equal(t, []string{"act.>"}, afterInfo.Config.Subjects, "DST must own the promoted subjects")
	assert.Equal(t, createdBefore, afterInfo.Created, "server stream must be the SAME stream (Created timestamp unchanged) — a delete+recreate would reset it")
	assert.EqualValues(t, 1, afterInfo.State.FirstSeq, "first sequence preserved — not a fresh seq-0 epoch")
}

// Test_streamPromoteInPlace_subjectOverlapIsRetryableNotDestructive proves the
// ordering safety: if the in-place promote runs WHILE the source still owns the
// subjects, nats-server rejects with 10065 (overlap). The fix classifies this as
// a transient/retryable condition — isSubjectOverlapErr(err) is true and
// isMirrorIncompatibleErr(err) is false — so the reconciler will NOT delete the
// stream. We then prove the same promote succeeds (data intact) once the source
// releases the subjects.
func Test_streamPromoteInPlace_subjectOverlapIsRetryableNotDestructive(t *testing.T) {
	mgr, js, cleanup := newJSTestServer(t)
	defer cleanup()

	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"act.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < 12; i++ {
		_, err := js.Publish("act.evt", []byte("p"))
		require.NoError(t, err)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: "DST", Mirror: &nats.StreamSource{Name: "ORIGIN"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo("DST")
		return e == nil && info.State.Msgs == 12
	}, 5*time.Second, 100*time.Millisecond)

	dst, err := mgr.LoadStream("DST")
	require.NoError(t, err)
	serverCfg := dst.Configuration()
	spec := &api.StreamSpec{Name: "DST", Subjects: []string{"act.>"}}
	opts, err := streamSpecToConfig(spec, &serverCfg)
	require.NoError(t, err)

	// Promote while ORIGIN still owns "act.>" → expect 10065 overlap.
	overlapErr := dst.UpdateConfiguration(serverCfg, opts...)
	require.Error(t, overlapErr, "promote must be rejected while source still owns subjects")
	assert.True(t, isSubjectOverlapErr(overlapErr), "overlap must be classified as the retryable 10065 condition")
	assert.False(t, isMirrorIncompatibleErr(overlapErr), "overlap must NOT be classified as a mirror-flip incompatibility (which would trigger destructive recreate)")

	// The stream must still exist with all its data (we did NOT delete it).
	stillThere, err := js.StreamInfo("DST")
	require.NoError(t, err)
	assert.EqualValues(t, 12, stillThere.State.Msgs, "data must be intact after the overlap rejection (no destructive recreate)")

	// Now the source demotes/releases subjects; the in-place promote succeeds.
	require.NoError(t, mgr.DeleteStream("ORIGIN"))
	dst2, err := mgr.LoadStream("DST")
	require.NoError(t, err)
	serverCfg2 := dst2.Configuration()
	opts2, err := streamSpecToConfig(spec, &serverCfg2)
	require.NoError(t, err)
	require.NoError(t, dst2.UpdateConfiguration(serverCfg2, opts2...), "promote must succeed once the source has released the subjects")

	final, err := js.StreamInfo("DST")
	require.NoError(t, err)
	assert.EqualValues(t, 12, final.State.Msgs, "all messages survive the eventually-successful in-place promote")
	assert.Nil(t, final.Config.Mirror)
}

// newKVTestServer spins up an embedded JetStream NATS server and returns a
// connected nats.go/jetstream.JetStream (the NEW API the KeyValue reconciler
// uses — CreateKeyValue / UpdateKeyValue / Stream) plus a jsm.Manager for the
// low-level identity probe. nats-server version is pinned by go.mod (v2.14.0).
func newKVTestServer(t *testing.T) (jetstream.JetStream, *jsm.Manager, func()) {
	t.Helper()
	opts := &natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	dir, err := os.MkdirTemp("", "nats-kv-promote-*")
	require.NoError(t, err)
	opts.StoreDir = dir

	srv := natsserver.RunServer(opts)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	mgr, err := jsm.New(nc)
	require.NoError(t, err)

	cleanup := func() {
		nc.Close()
		srv.Shutdown()
		os.RemoveAll(dir)
	}
	return js, mgr, cleanup
}

// Test_keyValuePromoteInPlace_preservesKeys is the KeyValue analog of
// Test_streamPromoteInPlace_preservesMessages, added because consumer-offsets
// is a KeyValue and a KV IS promoted on every DRP flip — so the data-
// preservation guarantee must be proven empirically against the real
// nats-server, not just inferred from code review.
//
// It drives the EXACT production KV path the reconciler runs at
// keyvalue_controller.go: build the targetConfig via keyValueSpecToConfig, then
// js.UpdateKeyValue(ctx, targetConfig) — NOT a hand-rolled UpdateStream.
//
// FIXTURE FIDELITY — why the source stream rewrites subjects to the dst bucket:
// In production the dst KV bucket mirrors a peer KV bucket with the SAME bucket
// name across regions (e.g. dev-east "consumer-offsets" mirrors dev-west
// "consumer-offsets"). A KV bucket stores its data under "$KV.<bucket>.>", so a
// same-name cross-region mirror replicates messages whose subjects already
// equal the dst bucket's own subjects. After promote, the dst stream's subject
// filter ("$KV.<bucket>.>") therefore matches the stored messages and the keys
// are readable. To reproduce that on a single embedded server (where two
// streams can't both be named KV_consumer-offsets), the source is a separate
// stream with a SubjectTransform that rewrites its subjects to
// "$KV.<dstBucket>.>" — the exact subject shape a same-name cross-region mirror
// yields. (Verified empirically: with a NAÏVE mirror of a differently-named
// bucket the keys are stored under the SOURCE's subjects and become invisible
// after promote — a fixture artifact, not a real bug; the production same-name
// mirror keeps subjects aligned.)
//
// Flow: source KV bucket with 10 distinct key→value pairs → real MIRROR bucket
// "consumer-offsets" (subjects transformed to its own) → flip the mirror's spec
// to primary (drop Mirror) → run the production UpdateKeyValue in place →
// assert all 10 keys + values survive AND are readable, the backing stream
// identity (Created) is unchanged (NOT recreated), Mirror==nil, subjects are
// the KV-standard "$KV.<bucket>.>".
func Test_keyValuePromoteInPlace_preservesKeys(t *testing.T) {
	js, mgr, cleanup := newKVTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const (
		sourceBucket = "consumer-offsets-peer" // stands in for the remote-region peer
		dstBucket    = "consumer-offsets"
		nKeys        = 10
	)

	// Peer KV bucket with 10 distinct key→value pairs (the data to preserve).
	source, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: sourceBucket})
	require.NoError(t, err)
	want := make(map[string]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("offset.%d", i)
		v := fmt.Sprintf("value-%d", i)
		_, perr := source.Put(ctx, k, []byte(v))
		require.NoError(t, perr)
		want[k] = v
	}

	// MIRROR KV bucket "consumer-offsets" that replicates the peer, rewriting
	// subjects "$KV.<peer>.>" → "$KV.consumer-offsets.>" so the replicated
	// messages land under the dst bucket's own subjects — exactly the subject
	// shape a same-name cross-region mirror produces.
	_, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: dstBucket,
		Mirror: &jetstream.StreamSource{
			Name: kvStreamPrefix + sourceBucket,
			SubjectTransforms: []jetstream.SubjectTransformConfig{
				{
					Source:      fmt.Sprintf("$KV.%s.>", sourceBucket),
					Destination: fmt.Sprintf("$KV.%s.>", dstBucket),
				},
			},
		},
	})
	require.NoError(t, err)

	dstStreamName := kvStreamPrefix + dstBucket
	require.Eventually(t, func() bool {
		s, e := js.Stream(ctx, dstStreamName)
		if e != nil {
			return false
		}
		info, ie := s.Info(ctx)
		return ie == nil && info.State.Msgs == nKeys
	}, 5*time.Second, 100*time.Millisecond, "mirror KV must replicate all 10 keys before promote")

	// Capture the backing stream's identity so we can prove it is NEVER
	// recreated by the promote (a delete+recreate would reset Created).
	beforeS, err := js.Stream(ctx, dstStreamName)
	require.NoError(t, err)
	beforeInfo, err := beforeS.Info(ctx)
	require.NoError(t, err)
	createdBefore := beforeInfo.Created
	require.NotNil(t, beforeInfo.Config.Mirror, "precondition: dst KV backing stream is a mirror before promote")

	// Source releases first (mirror→primary demote on the peer side), exactly
	// as DemotingSource does before PromotingDestination — otherwise the promote
	// would hit 10065 subject-overlap.
	require.NoError(t, mgr.DeleteStream(kvStreamPrefix+sourceBucket))

	// THE PROMOTE — in place, via the EXACT production calls.
	dstKVSpec := &api.KeyValueSpec{Bucket: dstBucket} // primary form: no Mirror.
	targetConfig, err := keyValueSpecToConfig(dstKVSpec)
	require.NoError(t, err)
	require.Nil(t, targetConfig.Mirror, "promote targetConfig must carry no mirror")
	_, err = js.UpdateKeyValue(ctx, targetConfig)
	require.NoError(t, err, "in-place mirror→primary KeyValue promote must be accepted by nats-server")

	// Assertion 1: all 10 keys still present with the correct values.
	promoted, err := js.KeyValue(ctx, dstBucket)
	require.NoError(t, err)
	keys, err := promoted.Keys(ctx)
	require.NoError(t, err)
	assert.Len(t, keys, nKeys, "all 10 keys must survive the in-place KV promote")
	for k, v := range want {
		entry, gerr := promoted.Get(ctx, k)
		require.NoError(t, gerr, "key %q must still exist after promote", k)
		assert.Equal(t, v, string(entry.Value()), "value for key %q must be preserved", k)
	}

	// Assertion 2: backing-stream identity unchanged (NOT recreated).
	afterS, err := js.Stream(ctx, dstStreamName)
	require.NoError(t, err)
	afterInfo, err := afterS.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, createdBefore, afterInfo.Created, "KV backing stream must be the SAME stream (Created unchanged) — a delete+recreate would reset it")
	assert.EqualValues(t, nKeys, afterInfo.State.Msgs, "all 10 messages must remain on the backing stream")

	// Assertion 3: Mirror dropped + subjects are the KV-standard form.
	assert.Nil(t, afterInfo.Config.Mirror, "dst KV must be primary-form (Mirror==nil) after promote")
	assert.Equal(t, []string{fmt.Sprintf("$KV.%s.>", dstBucket)}, afterInfo.Config.Subjects, "promoted KV backing stream must own the standard $KV.<bucket>.> subjects")
}

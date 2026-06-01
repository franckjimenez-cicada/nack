/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/jsm.go"
	jsmapi "github.com/nats-io/jsm.go/api"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

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

/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"errors"
	"fmt"
	"testing"

	jsmapi "github.com/nats-io/jsm.go/api"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats.go/jetstream"
	v1 "k8s.io/api/core/v1"
)

func TestStreamMirrorFlipped(t *testing.T) {
	cases := []struct {
		name        string
		serverState *jsmapi.StreamConfig
		spec        *api.StreamSpec
		want        bool
	}{
		{
			name:        "no server state -> no flip",
			serverState: nil,
			spec:        &api.StreamSpec{Mirror: &api.StreamSource{Name: "src"}},
			want:        false,
		},
		{
			name:        "nil spec -> no flip",
			serverState: &jsmapi.StreamConfig{Mirror: &jsmapi.StreamSource{Name: "src"}},
			spec:        nil,
			want:        false,
		},
		{
			name:        "both source -> no flip",
			serverState: &jsmapi.StreamConfig{Subjects: []string{"foo.>"}},
			spec:        &api.StreamSpec{Subjects: []string{"foo.>"}},
			want:        false,
		},
		{
			name:        "both mirror -> no flip",
			serverState: &jsmapi.StreamConfig{Mirror: &jsmapi.StreamSource{Name: "src"}},
			spec:        &api.StreamSpec{Mirror: &api.StreamSource{Name: "src"}},
			want:        false,
		},
		{
			name:        "server source, spec mirror -> flip",
			serverState: &jsmapi.StreamConfig{Subjects: []string{"foo.>"}},
			spec:        &api.StreamSpec{Mirror: &api.StreamSource{Name: "src"}},
			want:        true,
		},
		{
			name:        "server mirror, spec source -> flip",
			serverState: &jsmapi.StreamConfig{Mirror: &jsmapi.StreamSource{Name: "src"}},
			spec:        &api.StreamSpec{Subjects: []string{"foo.>"}},
			want:        true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := streamMirrorFlipped(c.serverState, c.spec)
			if got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestKeyValueMirrorFlipped(t *testing.T) {
	cases := []struct {
		name        string
		serverState *jetstream.StreamConfig
		spec        *api.KeyValueSpec
		want        bool
	}{
		{
			name:        "no server state -> no flip",
			serverState: nil,
			spec:        &api.KeyValueSpec{Mirror: &api.StreamSource{Name: "src"}},
			want:        false,
		},
		{
			name:        "server source, spec mirror -> flip",
			serverState: &jetstream.StreamConfig{},
			spec:        &api.KeyValueSpec{Mirror: &api.StreamSource{Name: "src"}},
			want:        true,
		},
		{
			name:        "server mirror, spec source -> flip",
			serverState: &jetstream.StreamConfig{Mirror: &jetstream.StreamSource{Name: "src"}},
			spec:        &api.KeyValueSpec{},
			want:        true,
		},
		{
			name:        "both mirror -> no flip",
			serverState: &jetstream.StreamConfig{Mirror: &jetstream.StreamSource{Name: "src"}},
			spec:        &api.KeyValueSpec{Mirror: &api.StreamSource{Name: "src"}},
			want:        false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := keyValueMirrorFlipped(c.serverState, c.spec)
			if got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestIsMirrorIncompatibleErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil -> false", nil, false},
		{"plain error -> false", errors.New("boom"), false},
		{
			"NATS 10055 -> true",
			fmt.Errorf("wrapped: %w", jsmapi.ApiError{ErrCode: JSStreamMirrorInvalidErr, Code: 500}),
			true,
		},
		{
			"NATS 10034 -> true",
			jsmapi.ApiError{ErrCode: JSStreamMirrorWithSubjectsErr, Code: 500},
			true,
		},
		{
			"NATS 10031 -> true",
			jsmapi.ApiError{ErrCode: JSStreamMirrorWithSourcesErr, Code: 500},
			true,
		},
		{
			"unrelated NATS error -> false",
			jsmapi.ApiError{ErrCode: JSStreamNotFoundErr, Code: 404},
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isMirrorIncompatibleErr(c.err)
			if got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestBackupConfirmedForGeneration(t *testing.T) {
	cases := []struct {
		name        string
		annotations map[string]string
		key         string
		gen         int64
		want        bool
	}{
		{"nil map", nil, "k", 7, false},
		{"key missing", map[string]string{"other": "7"}, "k", 7, false},
		{"value empty", map[string]string{"k": ""}, "k", 7, false},
		{"value mismatch", map[string]string{"k": "8"}, "k", 7, false},
		{"value match", map[string]string{"k": "7"}, "k", 7, true},
		{"non-numeric value mismatch", map[string]string{"k": "seven"}, "k", 7, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := backupConfirmedForGeneration(c.annotations, c.key, c.gen)
			if got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestRemoveBackupRequiredCondition(t *testing.T) {
	in := []api.Condition{
		{Type: readyCondType, Status: v1.ConditionTrue},
		{Type: conditionBackupRequired, Status: v1.ConditionTrue, Reason: "PeerStale"},
		{Type: "Other", Status: v1.ConditionFalse},
	}
	out := removeBackupRequiredCondition(in)
	for _, c := range out {
		if c.Type == conditionBackupRequired {
			t.Fatalf("removeBackupRequiredCondition kept the gate condition: %+v", c)
		}
	}
	// other conditions preserved + order preserved
	if len(out) != 2 {
		t.Fatalf("expected 2 remaining conditions, got %d (%+v)", len(out), out)
	}
	if out[0].Type != readyCondType || out[1].Type != "Other" {
		t.Fatalf("order not preserved: %+v", out)
	}
}

func TestUpdateBackupRequiredCondition_UpsertSemantics(t *testing.T) {
	// First write seeds the condition.
	first := updateBackupRequiredCondition(nil, v1.ConditionTrue, "PeerStale", "msg-1")
	if len(first) != 1 || first[0].Type != conditionBackupRequired || first[0].Status != v1.ConditionTrue || first[0].Reason != "PeerStale" {
		t.Fatalf("first upsert wrong: %+v", first)
	}
	// Second write w/ same status keeps the transition time stable.
	originalTime := first[0].LastTransitionTime
	second := updateBackupRequiredCondition(first, v1.ConditionTrue, "PeerStale", "msg-2")
	if second[0].LastTransitionTime != originalTime {
		t.Fatalf("transition time should be stable when status unchanged; before=%s after=%s", originalTime, second[0].LastTransitionTime)
	}
	if second[0].Message != "msg-2" {
		t.Fatalf("message not updated: %+v", second[0])
	}
	// Third write w/ different status bumps the transition time.
	third := updateBackupRequiredCondition(second, v1.ConditionFalse, "Done", "cleared")
	if third[0].LastTransitionTime == originalTime {
		t.Fatalf("transition time should advance on status change; got %s", third[0].LastTransitionTime)
	}
}

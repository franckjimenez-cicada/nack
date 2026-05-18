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

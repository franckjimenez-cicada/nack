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

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// passiveRoleGate is the minimum surface shouldTranslatePassiveRole
// needs. Kept narrow so unit tests can stand up a fake without
// implementing the full JetStreamController interface — *jsController
// satisfies it transitively via its embedded client.Client + the new
// methods, and tests can pass a fake-client-backed stub.
type passiveRoleGate interface {
	client.Reader
	PassiveRoleTranslationEnabled() bool
	CrossRegionNATSDomain() string
}

// shouldTranslatePassiveRole reports whether the reconciler should rewrite
// the supplied CR's spec to mirror form before applying to NATS.
//
// The translation fires only when ALL of the following hold:
//   - The controller has --enable-passive-role-translation set (feature gate).
//   - --cross-region-nats-domain is non-empty (we need it to build the
//     externalApiPrefix). Without it, translation would synthesize an
//     invalid mirror config — better to skip and leave the CR as authored.
//   - The CR's namespace carries `drp.cicada.io/local-role=passive`.
//
// On any error reading the namespace (network blip, RBAC gap), the
// function returns (false, err) so the caller can decide whether to fail
// the reconcile or fall through to non-translated behavior. The intent is
// "safe default = no translation" — a failure to read the role MUST NOT
// silently convert a primary stream to mirror.
func shouldTranslatePassiveRole(ctx context.Context, g passiveRoleGate, namespace string) (bool, error) {
	if !g.PassiveRoleTranslationEnabled() {
		return false, nil
	}
	if g.CrossRegionNATSDomain() == "" {
		return false, nil
	}

	ns := &corev1.Namespace{}
	if err := g.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read namespace %q to evaluate local-role: %w", namespace, err)
	}

	return ns.Annotations[localRoleAnnotation] == localRolePassive, nil
}

// translateStreamSpecToMirror returns a copy of the supplied Stream spec
// with Subjects cleared and Mirror set to a config that pulls from the
// peer region's JetStream domain. The original spec (and therefore the
// in-cluster CR) is left untouched.
//
// Caller is responsible for already having decided translation should
// fire (see shouldTranslatePassiveRole) — this function performs the
// transformation unconditionally on its inputs.
func translateStreamSpecToMirror(orig *api.StreamSpec, remoteDomain string) api.StreamSpec {
	translated := *orig
	translated.Subjects = nil
	translated.Sources = nil
	streamName := orig.Name
	translated.Mirror = &api.StreamSource{
		Name:                  streamName,
		ExternalAPIPrefix:     fmt.Sprintf("$JS.%s.API", remoteDomain),
		ExternalDeliverPrefix: fmt.Sprintf("deliver.%s.dr", streamName),
	}
	return translated
}

// translateKeyValueSpecToMirror is the KeyValue analog. The underlying
// JetStream stream that backs a KV bucket is named "KV_<bucket>", so the
// mirror's Name field uses that convention. The deliver prefix follows
// the chart's "deliver.kv.<bucket>.dr" pattern documented in
// gitops-platform-dev-stg/children/nacks-streams-sync values.
func translateKeyValueSpecToMirror(orig *api.KeyValueSpec, remoteDomain string) api.KeyValueSpec {
	translated := *orig
	translated.Sources = nil
	bucket := orig.Bucket
	translated.Mirror = &api.StreamSource{
		Name:                  kvStreamPrefix + bucket,
		ExternalAPIPrefix:     fmt.Sprintf("$JS.%s.API", remoteDomain),
		ExternalDeliverPrefix: fmt.Sprintf("deliver.kv.%s.dr", bucket),
	}
	return translated
}

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package webhook implements admission webhooks for nack CRDs.
//
// The SiblingConflict validators reject CREATE/UPDATE of Stream/KeyValue CRs
// whose spec.name (or spec.mirror.name) collides with another CR of the same
// kind in the same namespace, UNLESS the namespace carries the drill-active
// annotation set by drp-operator during a coordinated DR drill.
//
// This stops the reconcile war that happens when an out-of-band actor (e.g.
// an ArgoCD app drifting to autosync during failback) recreates mirror CRs
// alongside their primaries. The fork already has --mirror-recreate-on-conflict
// for the steady-state case; this webhook is the admission-time guard so the
// problem never reaches the controller at all.
package webhook

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// DrillActiveAnnotation, when set on the target namespace, suppresses the
// sibling-conflict rejection. The drp-operator sets this for the duration of
// a drill (value = drillID for audit), then clears it on completion.
const DrillActiveAnnotation = "drp.cicada.io/drill-active"

// NATSAccountLabel identifies which NATS account a Stream/KeyValue CR
// targets. Two CRs with the same `spec.name` but DIFFERENT account labels
// resolve to different server-side streams (one per account) and therefore
// do NOT conflict at the NATS layer.
//
// Our chart sets values like "JS" or "nats-qa". The label is OPTIONAL: when
// absent on either side, we fall back to the conservative legacy behavior
// (treat as potential conflict) so this change is backward-compatible.
const NATSAccountLabel = "drp.cicada.io/nats-account"

// accountLabel returns (value, present) for the NATS account label on an
// object's metadata. A nil map or missing key returns ("", false).
func accountLabel(labels map[string]string) (string, bool) {
	if labels == nil {
		return "", false
	}
	v, ok := labels[NATSAccountLabel]
	return v, ok && v != ""
}

// differentAccounts returns true iff BOTH sides carry the account label and
// the values differ. When either side is unlabeled we return false so the
// caller falls through to the legacy conflict logic (conservative default).
func differentAccounts(selfLabels, otherLabels map[string]string) bool {
	selfAcc, selfOK := accountLabel(selfLabels)
	otherAcc, otherOK := accountLabel(otherLabels)
	if !selfOK || !otherOK {
		return false
	}
	return selfAcc != otherAcc
}

// remediationHint is appended to every rejection so operators understand
// what to do — the most common cause is an ArgoCD app drifting to autosync.
const remediationHint = "Only the drp-operator may create conflicting CRs during a coordinated drill. " +
	"If this CR is the wanted state, delete the conflicting sibling first. " +
	"If you're testing operator code locally, set the '" + DrillActiveAnnotation + "' annotation on the namespace."

// drillActive returns true when the namespace carries the drill-active
// annotation with a non-empty value. A nil client (unit-test path with a
// pre-seeded namespace lookup) is treated as no drill.
func drillActive(ctx context.Context, c ctrlclient.Client, namespace string) (bool, string, error) {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return false, "", nil
		}
		return false, "", fmt.Errorf("get namespace %q: %w", namespace, err)
	}
	v, ok := ns.Annotations[DrillActiveAnnotation]
	if !ok || v == "" {
		return false, "", nil
	}
	return true, v, nil
}

// streamSpecName extracts the on-wire stream name a Stream CR resolves to.
// For a primary it is spec.name; for a mirror it is still spec.name (the
// local mirror name == the source name by NACK convention).
func streamSpecName(s *api.Stream) string {
	return s.Spec.Name
}

// streamMirrorSource returns the name of the source stream this CR mirrors,
// or "" when the CR is not a mirror.
func streamMirrorSource(s *api.Stream) string {
	if s.Spec.Mirror == nil {
		return ""
	}
	return s.Spec.Mirror.Name
}

// keyValueSpecName returns the bucket name for a KeyValue CR. KV buckets
// materialize as KV_<bucket> streams in NATS, but the K8s-level conflict
// surface is the bucket field itself.
func keyValueSpecName(kv *api.KeyValue) string {
	return kv.Spec.Bucket
}

// findStreamConflict returns the first sibling Stream CR in the same
// namespace that collides with `self`, OR nil. Collision rules:
//
//  1. another CR has the same spec.name and a different metadata.name (the
//     classic primary-vs-mirror-with-same-on-wire-name)
//  2. another CR is a mirror whose spec.mirror.name equals self.spec.name
//     (the exact ArgoCD-drift scenario from 2026-05-24: a freshly synced
//     mirror CR pointing back at a primary that's already live)
//  3. self IS a mirror whose spec.mirror.name equals another sibling's
//     spec.name (symmetric: catches it from the other direction too)
func findStreamConflict(ctx context.Context, c ctrlclient.Client, self *api.Stream) (*api.Stream, string, error) {
	list := &api.StreamList{}
	if err := c.List(ctx, list, ctrlclient.InNamespace(self.Namespace)); err != nil {
		return nil, "", fmt.Errorf("list streams in %q: %w", self.Namespace, err)
	}

	selfName := streamSpecName(self)
	selfMirror := streamMirrorSource(self)

	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == self.Name {
			continue // self
		}

		// account-aware filter: same spec.name across different NATS
		// accounts is NOT a conflict (separate server-side streams).
		// Only applies when BOTH CRs are labeled; absent labels fall
		// through to the legacy logic (backward compatible).
		if differentAccounts(self.Labels, other.Labels) {
			continue
		}

		otherName := streamSpecName(other)
		otherMirror := streamMirrorSource(other)

		// rule 1: same on-wire name
		if selfName != "" && otherName == selfName {
			return other, fmt.Sprintf("sibling Stream %q already declares spec.name=%q", other.Name, otherName), nil
		}
		// rule 2: sibling is a mirror of self's spec.name
		if selfName != "" && otherMirror == selfName {
			return other, fmt.Sprintf("sibling Stream %q is a mirror of spec.name=%q", other.Name, selfName), nil
		}
		// rule 3: self is a mirror of sibling's spec.name
		if selfMirror != "" && otherName == selfMirror {
			return other, fmt.Sprintf("self mirrors %q which is also declared as spec.name by sibling Stream %q", selfMirror, other.Name), nil
		}
	}
	return nil, "", nil
}

// findKeyValueConflict applies the spec.bucket variant of findStreamConflict.
// KV mirroring rules are simpler — only bucket-vs-bucket collisions are
// checked because KV mirrors reuse the same bucket name verbatim.
func findKeyValueConflict(ctx context.Context, c ctrlclient.Client, self *api.KeyValue) (*api.KeyValue, string, error) {
	list := &api.KeyValueList{}
	if err := c.List(ctx, list, ctrlclient.InNamespace(self.Namespace)); err != nil {
		return nil, "", fmt.Errorf("list keyvalues in %q: %w", self.Namespace, err)
	}

	selfBucket := keyValueSpecName(self)
	if selfBucket == "" {
		return nil, "", nil
	}

	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == self.Name {
			continue
		}
		// account-aware filter (see findStreamConflict for rationale).
		if differentAccounts(self.Labels, other.Labels) {
			continue
		}
		if keyValueSpecName(other) == selfBucket {
			return other, fmt.Sprintf("sibling KeyValue %q already declares spec.bucket=%q", other.Name, selfBucket), nil
		}
	}
	return nil, "", nil
}

// rejectionError formats a sibling-conflict rejection consistently.
func rejectionError(gvk schema.GroupVersionKind, self, sibling, reason string) error {
	return fmt.Errorf(
		"%s %q rejected by sibling-conflict webhook: %s. %s",
		gvk.Kind, self, reason, remediationHint,
	)
}

// StreamValidator implements admission.Validator for Stream CRs.
type StreamValidator struct {
	Client ctrlclient.Client
}

var _ admission.Validator[*api.Stream] = &StreamValidator{}

func (v *StreamValidator) ValidateCreate(ctx context.Context, obj *api.Stream) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

func (v *StreamValidator) ValidateUpdate(ctx context.Context, _ *api.Stream, obj *api.Stream) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

func (v *StreamValidator) ValidateDelete(_ context.Context, _ *api.Stream) (admission.Warnings, error) {
	return nil, nil
}

func (v *StreamValidator) validate(ctx context.Context, obj *api.Stream) (admission.Warnings, error) {
	// Step 1: operator-only gate (drill-active + scope-labeled + non-operator → REJECT).
	// Runs FIRST so a denied request never burns the sibling-list cost.
	// See drill_active_gate.go's header for the full rationale (live failure
	// 2026-05-29 on the E→W flip; 7/17 streams failed promote).
	if allowed, _, denyReason, err := drillActiveOperatorGate(ctx, v.Client, obj); err != nil {
		return nil, err
	} else if !allowed {
		return nil, formatOperatorOnlyRejection("Stream", obj.Name, denyReason)
	}

	// Step 2: legacy sibling-conflict check. Unchanged behavior — the
	// operator-only gate fires only inside drill windows on scope-labeled
	// CRs; everything else (chart steady-state, manual kubectl outside a
	// drill, drill-active without scope label) falls through here.
	sibling, reason, err := findStreamConflict(ctx, v.Client, obj)
	if err != nil {
		return nil, err
	}
	if sibling == nil {
		return nil, nil
	}

	active, drillID, err := drillActive(ctx, v.Client, obj.Namespace)
	if err != nil {
		return nil, err
	}
	if active {
		return admission.Warnings{
			fmt.Sprintf("sibling conflict allowed during drill %q: %s", drillID, reason),
		}, nil
	}

	return nil, rejectionError(
		schema.GroupVersionKind{Group: api.SchemeGroupVersion.Group, Version: api.SchemeGroupVersion.Version, Kind: "Stream"},
		obj.Name, sibling.Name, reason,
	)
}

// KeyValueValidator implements admission.Validator for KeyValue CRs.
type KeyValueValidator struct {
	Client ctrlclient.Client
}

var _ admission.Validator[*api.KeyValue] = &KeyValueValidator{}

func (v *KeyValueValidator) ValidateCreate(ctx context.Context, obj *api.KeyValue) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

func (v *KeyValueValidator) ValidateUpdate(ctx context.Context, _ *api.KeyValue, obj *api.KeyValue) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

func (v *KeyValueValidator) ValidateDelete(_ context.Context, _ *api.KeyValue) (admission.Warnings, error) {
	return nil, nil
}

func (v *KeyValueValidator) validate(ctx context.Context, obj *api.KeyValue) (admission.Warnings, error) {
	// Step 1: operator-only gate — same rationale as StreamValidator. See
	// drill_active_gate.go header for the live failure context.
	if allowed, _, denyReason, err := drillActiveOperatorGate(ctx, v.Client, obj); err != nil {
		return nil, err
	} else if !allowed {
		return nil, formatOperatorOnlyRejection("KeyValue", obj.Name, denyReason)
	}

	// Step 2: legacy sibling-conflict check.
	sibling, reason, err := findKeyValueConflict(ctx, v.Client, obj)
	if err != nil {
		return nil, err
	}
	if sibling == nil {
		return nil, nil
	}

	active, drillID, err := drillActive(ctx, v.Client, obj.Namespace)
	if err != nil {
		return nil, err
	}
	if active {
		return admission.Warnings{
			fmt.Sprintf("sibling conflict allowed during drill %q: %s", drillID, reason),
		}, nil
	}

	return nil, rejectionError(
		schema.GroupVersionKind{Group: api.SchemeGroupVersion.Group, Version: api.SchemeGroupVersion.Version, Kind: "KeyValue"},
		obj.Name, sibling.Name, reason,
	)
}

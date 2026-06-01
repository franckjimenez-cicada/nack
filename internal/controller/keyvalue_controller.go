/*
Copyright 2025.

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

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats.go/jetstream"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	kvStreamPrefix = "KV_"
)

// KeyValueReconciler reconciles a KeyValue object
type KeyValueReconciler struct {
	Scheme *runtime.Scheme
	JetStreamController
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// It performs three main operations:
// - Initialize finalizer and ready condition if not present
// - Delete KeyValue if it is marked for deletion.
// - Create or Update the KeyValue
//
// A call to reconcile may perform only one action, expecting the reconciliation to be triggered again by an update.
// For example: Setting the finalizer triggers a second reconciliation. Reconcile returns after setting the finalizer,
// to prevent parallel reconciliations performing the same steps.
func (r *KeyValueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := klog.FromContext(ctx)

	if ok := r.ValidNamespace(req.Namespace); !ok {
		log.Info("Controller restricted to namespace, skipping reconciliation.")
		return ctrl.Result{}, nil
	}

	// Fetch KeyValue resource
	keyValue := &api.KeyValue{}
	if err := r.Get(ctx, req.NamespacedName, keyValue); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("KeyValue resource deleted.", "keyValueName", req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get keyvalue resource '%s': %w", req.NamespacedName.String(), err)
	}

	log = log.WithValues("keyValueName", keyValue.Spec.Bucket)

	// Update ready status to unknown when no status is set
	if len(keyValue.Status.Conditions) == 0 {
		log.Info("Setting initial ready condition to unknown.")
		keyValue.Status.Conditions = updateReadyCondition(keyValue.Status.Conditions, v1.ConditionUnknown, stateReconciling, "Starting reconciliation")
		err := r.Status().Update(ctx, keyValue)
		if err != nil {
			// If we get a conflict error, another reconciliation has already updated the status.
			// Just requeue and let the next reconciliation handle it.
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, fmt.Errorf("set condition unknown: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check Deletion
	markedForDeletion := keyValue.GetDeletionTimestamp() != nil
	if markedForDeletion {
		if controllerutil.ContainsFinalizer(keyValue, keyValueFinalizer) {
			err := r.deleteKeyValue(ctx, log, keyValue)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete keyvalue: %w", err)
			}
		} else {
			log.Info("KeyValue marked for deletion and already finalized. Ignoring.")
		}

		return ctrl.Result{}, nil
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(keyValue, keyValueFinalizer) {
		log.Info("Adding KeyValue finalizer.")
		if ok := controllerutil.AddFinalizer(keyValue, keyValueFinalizer); !ok {
			return ctrl.Result{}, errors.New("failed to add finalizer to keyvalue resource")
		}

		if err := r.Update(ctx, keyValue); err != nil {
			return ctrl.Result{}, fmt.Errorf("update keyvalue resource to add finalizer: %w", err)
		}
		// After we have added the finalizer, we need to requeue to make sure we reconcile the
		// rest of the object. Just updating metadata won't make the API server update generation
		// so the update above shouldn't trigger a new reconciliation.
		return ctrl.Result{Requeue: true}, nil
	}

	// Create or update KeyValue
	if err := r.createOrUpdate(ctx, log, keyValue); err != nil {
		return ctrl.Result{}, fmt.Errorf("create or update: %s", err)
	}

	return ctrl.Result{RequeueAfter: r.RequeueInterval()}, nil
}

func (r *KeyValueReconciler) deleteKeyValue(ctx context.Context, log logr.Logger, keyValue *api.KeyValue) error {
	// Set status to false
	keyValue.Status.Conditions = updateReadyCondition(keyValue.Status.Conditions, v1.ConditionFalse, stateFinalizing, "Performing finalizer operations.")
	if err := r.Status().Update(ctx, keyValue); err != nil {
		return fmt.Errorf("update ready condition: %w", err)
	}

	storedState, err := getStoredKeyValueState(keyValue)
	if err != nil {
		log.Error(err, "Failed to fetch stored state.")
	}

	if !keyValue.Spec.PreventDelete && !r.ReadOnly() {
		log.Info("Deleting KeyValue.")
		err := r.WithJetStreamClient(keyValue.Spec.ConnectionOpts, keyValue.Namespace, func(js jetstream.JetStream) error {
			_, err := getServerKeyValueState(ctx, js, keyValue)
			// If we have no known state for this KeyValue it has never been reconciled.
			// If we are also receiving an error fetching state, either the KeyValue does not exist
			// or this resource config is invalid.
			if err != nil && storedState == nil {
				return nil
			}

			return js.DeleteKeyValue(ctx, keyValue.Spec.Bucket)
		})
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			log.Info("KeyValue does not exist, unable to delete.", "keyValueName", keyValue.Spec.Bucket)
		} else if err != nil && storedState == nil {
			log.Info("KeyValue not reconciled and no state received from server. Removing finalizer.")
		} else if err != nil {
			return fmt.Errorf("delete keyvalue during finalization: %w", err)
		}
	} else {
		log.Info("Skipping KeyValue deletion.",
			"preventDelete", keyValue.Spec.PreventDelete,
			"read-only", r.ReadOnly(),
		)
	}

	log.Info("Removing KeyValue finalizer.")
	if ok := controllerutil.RemoveFinalizer(keyValue, keyValueFinalizer); !ok {
		return errors.New("failed to remove keyvalue finalizer")
	}
	if err := r.Update(ctx, keyValue); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}

	return nil
}

func (r *KeyValueReconciler) createOrUpdate(ctx context.Context, log logr.Logger, keyValue *api.KeyValue) error {
	// Evaluate passive-role translation BEFORE any NATS interaction so
	// the rest of the reconcile (mirror-flip detection, backup gate,
	// UpdateKeyValue) all see the same "effective" spec. The K8s CR is
	// NOT modified — translation is server-side only. See
	// stream_controller.go for the matching block + full rationale,
	// including the B1 mirror→primary safety guard.
	translatePassive, localRole, translateErr := shouldTranslatePassiveRole(ctx, r.JetStreamController, keyValue.Namespace)
	if translateErr != nil {
		keyValue.Status.Conditions = updateReadyCondition(keyValue.Status.Conditions, v1.ConditionFalse, stateErrored, translateErr.Error())
		if err := r.Status().Update(ctx, keyValue); err != nil {
			log.Error(err, "Failed to update ready condition to Errored.")
		}
		return translateErr
	}
	effectiveSpec := &keyValue.Spec
	if translatePassive {
		translated := translateKeyValueSpecToMirror(&keyValue.Spec, r.CrossRegionNATSDomain())
		effectiveSpec = &translated
		log.Info("Passive-role translation active: applying mirror config to NATS server (K8s CR untouched).",
			"localRole", localRolePassive,
			"remoteDomain", r.CrossRegionNATSDomain(),
			"namespace", keyValue.Namespace,
			"bucket", keyValue.Spec.Bucket,
		)
	}

	// Create or Update the KeyValue based on the spec
	// Map effective spec (possibly translated) to KeyValue targetConfig
	targetConfig, err := keyValueSpecToConfig(effectiveSpec)
	if err != nil {
		return fmt.Errorf("map spec to keyvalue targetConfig: %w", err)
	}

	// gated mirrors the Stream reconciler — see stream_controller.go
	// for the BackupRequired gate semantics. The KV's underlying NATS
	// stream is "KV_<bucket>"; the probe + msg-count check use that.
	var gated bool
	var gatedReason, gatedMessage string
	var passiveRoleGuardBlocked bool
	var passiveRoleGuardMessage string

	// UpdateKeyValue is called on every reconciliation when the stream is not to be deleted.
	err = r.WithJetStreamClient(keyValue.Spec.ConnectionOpts, keyValue.Namespace, func(js jetstream.JetStream) error {
		storedState, err := getStoredKeyValueState(keyValue)
		if err != nil {
			log.Error(err, "Failed to fetch stored KeyValue state")
		}

		serverState, err := getServerKeyValueState(ctx, js, keyValue)
		if err != nil {
			return err
		}

		// Proactive source<->mirror flip handling. See the matching block in
		// stream_controller.go for the rationale.
		//
		// Use the EFFECTIVE spec (possibly translated to mirror form) so
		// passive-role translation triggers the destructive recreate path
		// when the server still holds a primary KV stream.
		if serverState != nil && !keyValue.Spec.PreventUpdate && !r.ReadOnly() && r.MirrorRecreateOnConflict() && keyValueMirrorFlipped(serverState, effectiveSpec) {
			// B1 safety guard — see stream_controller.go for rationale.
			if passiveRoleWouldDemote(serverState.Mirror != nil, effectiveSpec.Mirror != nil, localRole) {
				passiveRoleGuardBlocked = true
				passiveRoleGuardMessage = passiveRoleGuardMsg(keyValue.Namespace, r.PassiveRoleTranslationEnabled(), r.CrossRegionNATSDomain())
				log.Error(nil, "Passive-role safety guard fired; skipping proactive destructive recreate.",
					"namespace", keyValue.Namespace, "bucket", keyValue.Spec.Bucket,
				)
				return nil
			}
			// DATA-PRESERVING mirror→primary promote (the data-loss fix). When
			// the server KV is a mirror and the effective spec drops the mirror
			// to become a primary bucket, do NOT delete: fall through to the
			// normal update path, where UpdateKeyValue applies the mirror-less
			// targetConfig in place and retains the keys. The destructive
			// delete is reserved for the genuinely-impossible primary→mirror
			// direction. The B1 passiveRoleWouldDemote guard above has already
			// run, so a flag-toggled-off-while-passive misconfig is still
			// refused before reaching here.
			if keyValueMirrorToPrimaryFlip(serverState, effectiveSpec) {
				log.Info("Mirror→primary KeyValue promote detected; converting IN PLACE (preserving keys) instead of delete+recreate.",
					"bucket", keyValue.Spec.Bucket,
					"translated", translatePassive,
				)
				// Leave serverState set so the update path runs UpdateKeyValue
				// (in place) rather than CreateKeyValue.
			} else {
				gateFires, reason, msg, gateErr := r.keyValueBackupGate(ctx, js, keyValue)
				if gateErr != nil {
					return fmt.Errorf("evaluate backup gate: %w", gateErr)
				}
				if gateFires {
					log.Info("Destructive recreate gated by --require-backup-confirmation; awaiting external backup.",
						"reason", reason, "message", msg,
					)
					gated = true
					gatedReason = reason
					gatedMessage = msg
					return nil
				}
				log.Info("Source<->mirror flip detected (primary→mirror); force-recreating KeyValue.",
					"specHasMirror", effectiveSpec.Mirror != nil,
					"serverHasMirror", serverState.Mirror != nil,
					"translated", translatePassive,
				)
				if delErr := js.DeleteKeyValue(ctx, keyValue.Spec.Bucket); delErr != nil && !errors.Is(delErr, jetstream.ErrBucketNotFound) {
					return fmt.Errorf("force-delete on mirror flip: %w", delErr)
				}
				serverState = nil
			}
		}

		// Check against known state. Skip Update if converged.
		// Storing returned state from the server avoids have to
		// check default values or call Update on already converged resources
		if storedState != nil && serverState != nil && keyValue.Status.ObservedGeneration == keyValue.Generation {
			diff := compareConfigState(storedState, serverState)

			if diff == "" {
				return nil
			}

			log.Info("KeyValue config drifted from desired state.", "diff", diff)
		}

		if r.ReadOnly() {
			log.Info("Skipping KeyValue creation or update.",
				"read-only", r.ReadOnly(),
			)
			return nil
		}

		var updatedKeyValue jetstream.KeyValue
		err = nil

		if serverState == nil {
			log.Info("Creating KeyValue.")
			updatedKeyValue, err = js.CreateKeyValue(ctx, targetConfig)
			if err != nil {
				return err
			}
		} else if !keyValue.Spec.PreventUpdate {
			log.Info("Updating KeyValue.")
			updatedKeyValue, err = js.UpdateKeyValue(ctx, targetConfig)
			if err != nil {
				// DATA-LOSS GUARD (the promote fix): a subject-overlap (10065)
				// during a mirror→primary KV promote is a transient ordering
				// condition (source has not released the backing stream's
				// subjects yet), NOT a mirror-flip incompatibility. Surface it
				// as a retryable error rather than force-deleting the bucket,
				// which would destroy the replicated keys. The DRP flip demotes
				// the source before promoting the destination, so the overlap
				// self-clears on a subsequent reconcile.
				if isSubjectOverlapErr(err) {
					log.Info("In-place KeyValue promote rejected as subject-overlap (10065); source not yet demoted. Retrying in place (NOT deleting — preserves data).",
						"bucket", keyValue.Spec.Bucket, "natsErr", err.Error(),
					)
					return fmt.Errorf("in-place mirror→primary promote of KeyValue %q blocked by subject overlap (10065): source still owns the subjects (demote not yet converged); will retry without destroying data: %w", keyValue.Spec.Bucket, err)
				}
				// Reactive fallback: recreate the underlying KV stream when
				// NATS rejects the update as mirror-incompatible.
				//
				// NEVER take this destructive branch for a mirror→primary
				// promote — that direction is achievable in place (drop Mirror)
				// and deleting would lose the replicated keys.
				if r.MirrorRecreateOnConflict() && isMirrorIncompatibleErr(err) && !keyValueMirrorToPrimaryFlip(serverState, effectiveSpec) {
					// B1 safety guard (reactive site) — see stream_controller.go
					// for full rationale, including the note on serverState
					// staleness across the UpdateKeyValue boundary.
					if passiveRoleWouldDemote(serverState.Mirror != nil, effectiveSpec.Mirror != nil, localRole) {
						passiveRoleGuardBlocked = true
						passiveRoleGuardMessage = passiveRoleGuardMsg(keyValue.Namespace, r.PassiveRoleTranslationEnabled(), r.CrossRegionNATSDomain())
						log.Error(nil, "Passive-role safety guard fired; skipping reactive destructive recreate.",
							"namespace", keyValue.Namespace, "bucket", keyValue.Spec.Bucket, "natsErr", err.Error(),
						)
						return nil
					}
					gateFires, reason, msg, gateErr := r.keyValueBackupGate(ctx, js, keyValue)
					if gateErr != nil {
						return fmt.Errorf("evaluate backup gate: %w", gateErr)
					}
					if gateFires {
						log.Info("Reactive recreate gated by --require-backup-confirmation; awaiting external backup.",
							"reason", reason, "message", msg, "natsErr", err.Error(),
						)
						gated = true
						gatedReason = reason
						gatedMessage = msg
						return nil
					}
					log.Info("UpdateKeyValue rejected by NATS as mirror-incompatible; force-recreating KeyValue.",
						"err", err.Error(),
					)
					if delErr := js.DeleteKeyValue(ctx, keyValue.Spec.Bucket); delErr != nil && !errors.Is(delErr, jetstream.ErrBucketNotFound) {
						return fmt.Errorf("force-delete after mirror-incompatible update: %w", delErr)
					}
					updatedKeyValue, err = js.CreateKeyValue(ctx, targetConfig)
					if err != nil {
						return fmt.Errorf("re-create after mirror-incompatible update: %w", err)
					}
				} else {
					return err
				}
			} else {
				refreshed, err := getServerKeyValueState(ctx, js, keyValue)
				if err != nil {
					log.Error(err, "Failed to fetch updated KeyValue state")
				} else {
					diff := compareConfigState(refreshed, serverState)
					log.Info("Updated KeyValue.", "diff", diff)
				}
			}
		} else {
			log.Info("Skipping KeyValue update.",
				"preventUpdate", keyValue.Spec.PreventUpdate,
			)
		}

		if updatedKeyValue != nil {
			// Store known state in annotation
			serverState, err = getServerKeyValueState(ctx, js, keyValue)
			if err != nil {
				return err
			}

			updatedState, err := json.Marshal(serverState)
			if err != nil {
				return err
			}

			if keyValue.Annotations == nil {
				keyValue.Annotations = map[string]string{}
			}
			keyValue.Annotations[stateAnnotationKV] = string(updatedState)

			return r.Update(ctx, keyValue)
		}

		return nil
	})
	// Bake the PassiveRoleTranslated audit condition BEFORE any branch
	// writes Status — see stream_controller.go for rationale.
	if translatePassive {
		keyValue.Status.Conditions = updatePassiveRoleTranslatedCondition(
			keyValue.Status.Conditions, v1.ConditionTrue,
			"PassiveRole",
			fmt.Sprintf("Translated to mirror from $JS.%s.API", r.CrossRegionNATSDomain()),
		)
	} else {
		keyValue.Status.Conditions = removePassiveRoleTranslatedCondition(keyValue.Status.Conditions)
	}

	if err != nil {
		err = fmt.Errorf("create or update keyvalue: %w", err)
		keyValue.Status.Conditions = updateReadyCondition(keyValue.Status.Conditions, v1.ConditionFalse, stateErrored, err.Error())
		if err := r.Status().Update(ctx, keyValue); err != nil {
			log.Error(err, "Failed to update ready condition to Errored.")
		}
		return err
	}

	if passiveRoleGuardBlocked {
		keyValue.Status.Conditions = updateReadyCondition(
			keyValue.Status.Conditions, v1.ConditionFalse, stateErrored, passiveRoleGuardMessage,
		)
		if err := r.Status().Update(ctx, keyValue); err != nil {
			return fmt.Errorf("update Ready=Errored after passive-role guard: %w", err)
		}
		return nil
	}

	if gated {
		keyValue.Status.Conditions = updateBackupRequiredCondition(
			keyValue.Status.Conditions, v1.ConditionTrue, gatedReason, gatedMessage,
		)
		keyValue.Status.Conditions = updateReadyCondition(
			keyValue.Status.Conditions, v1.ConditionFalse, stateWaitingForBackup, gatedMessage,
		)
		if err := r.Status().Update(ctx, keyValue); err != nil {
			return fmt.Errorf("update BackupRequired condition: %w", err)
		}
		return nil
	}

	keyValue.Status.Conditions = removeBackupRequiredCondition(keyValue.Status.Conditions)

	// update the observed generation and ready status
	keyValue.Status.ObservedGeneration = keyValue.Generation
	keyValue.Status.Conditions = updateReadyCondition(
		keyValue.Status.Conditions,
		v1.ConditionTrue,
		stateReady,
		"KeyValue successfully created or updated.",
	)
	err = r.Status().Update(ctx, keyValue)
	if err != nil {
		return fmt.Errorf("update ready condition: %w", err)
	}

	return nil
}

func getStoredKeyValueState(keyValue *api.KeyValue) (*jetstream.StreamConfig, error) {
	var storedState *jetstream.StreamConfig
	if state, ok := keyValue.Annotations[stateAnnotationKV]; ok {
		err := json.Unmarshal([]byte(state), &storedState)
		if err != nil {
			return nil, err
		}
	}

	return storedState, nil
}

// Fetch the current state of the KeyValue stream from the server.
// ErrStreamNotFound is considered a valid response and does not return error
func getServerKeyValueState(ctx context.Context, js jetstream.JetStream, keyValue *api.KeyValue) (*jetstream.StreamConfig, error) {
	s, err := js.Stream(ctx, fmt.Sprintf("%s%s", kvStreamPrefix, keyValue.Spec.Bucket))
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &s.CachedInfo().Config, nil
}

// keyValueSpecToConfig creates a jetstream.KeyValueConfig matching the given KeyValue resource spec
func keyValueSpecToConfig(spec *api.KeyValueSpec) (jetstream.KeyValueConfig, error) {
	// Set directly mapped fields
	config := jetstream.KeyValueConfig{
		Bucket:         spec.Bucket,
		Compression:    spec.Compression,
		Description:    spec.Description,
		History:        uint8(spec.History),
		MaxBytes:       int64(spec.MaxBytes),
		MaxValueSize:   int32(spec.MaxValueSize),
		Replicas:       spec.Replicas,
		LimitMarkerTTL: spec.LimitMarkerTTL,
	}

	// TTL
	if spec.TTL != "" {
		t, err := time.ParseDuration(spec.TTL)
		if err != nil {
			return jetstream.KeyValueConfig{}, fmt.Errorf("invalid ttl: %w", err)
		}
		config.TTL = t
	}

	// storage
	if spec.Storage != "" {
		err := config.Storage.UnmarshalJSON(jsonString(spec.Storage))
		if err != nil {
			return jetstream.KeyValueConfig{}, fmt.Errorf("invalid storage: %w", err)
		}
	}

	// placement
	if spec.Placement != nil {
		config.Placement = &jetstream.Placement{
			Cluster: spec.Placement.Cluster,
			Tags:    spec.Placement.Tags,
		}
	}

	// mirror
	if spec.Mirror != nil {
		ss, err := mapStreamSource(spec.Mirror)
		if err != nil {
			return jetstream.KeyValueConfig{}, fmt.Errorf("map mirror keyvalue source: %w", err)
		}
		config.Mirror = ss
	}

	// sources
	if spec.Sources != nil {
		config.Sources = []*jetstream.StreamSource{}
		for _, source := range spec.Sources {
			s, err := mapStreamSource(source)
			if err != nil {
				return jetstream.KeyValueConfig{}, fmt.Errorf("map keyvalue source: %w", err)
			}
			config.Sources = append(config.Sources, s)
		}
	}

	// RePublish
	if spec.RePublish != nil {
		config.RePublish = &jetstream.RePublish{
			Source:      spec.RePublish.Source,
			Destination: spec.RePublish.Destination,
			HeadersOnly: spec.RePublish.HeadersOnly,
		}
	}

	return config, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KeyValueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&api.KeyValue{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Complete(r)
}

// keyValueBackupGate mirrors streamBackupGate for KV. The underlying
// NATS stream is "KV_<bucket>" and that's what the cross-region probe
// queries; from the operator's perspective the safety story is
// identical to a regular Stream.
func (r *KeyValueReconciler) keyValueBackupGate(ctx context.Context, js jetstream.JetStream, keyValue *api.KeyValue) (gateFires bool, reason, message string, gateErr error) {
	if !r.RequireBackupConfirmation() {
		return false, "", "", nil
	}
	bucket := keyValue.Spec.Bucket
	streamName := kvStreamPrefix + bucket
	s, err := js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return false, "", "", nil
		}
		return false, "", "", fmt.Errorf("load KV stream for state: %w", err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return false, "", "", fmt.Errorf("read KV stream info: %w", err)
	}
	if info.State.Msgs == 0 {
		return false, "", "", nil
	}
	if backupConfirmedForGeneration(keyValue.Annotations, r.BackupConfirmedAnnotation(), keyValue.Generation) {
		return false, "", "", nil
	}
	if url := r.CrossRegionNATSURL(); url != "" {
		peerMsgs, exists, probeErr := probeCrossRegionStreamMsgs(url, r.CrossRegionNATSCredsPath(), streamName, 5*time.Second)
		if probeErr == nil && exists && peerMsgs >= info.State.Msgs {
			return false, "", "", nil
		}
		switch {
		case probeErr != nil:
			return true, "PeerUnreachable",
				fmt.Sprintf("local KV %q has %d messages; cross-region probe failed: %v", bucket, info.State.Msgs, probeErr), nil
		case !exists:
			return true, "PeerMissing",
				fmt.Sprintf("local KV %q has %d messages; cross-region peer stream %q not found", bucket, info.State.Msgs, streamName), nil
		default:
			return true, "PeerStale",
				fmt.Sprintf("local KV %q has %d messages; cross-region peer has %d", bucket, info.State.Msgs, peerMsgs), nil
		}
	}
	return true, "NoProbeConfigured",
		fmt.Sprintf("local KV %q has %d messages and --cross-region-nats-url is unset", bucket, info.State.Msgs), nil
}

// keyValueMirrorFlipped reports whether the desired KeyValue spec disagrees
// with the server-side KV stream config on whether it is a mirror.
func keyValueMirrorFlipped(serverState *jetstream.StreamConfig, spec *api.KeyValueSpec) bool {
	if serverState == nil || spec == nil {
		return false
	}
	serverHasMirror := serverState.Mirror != nil
	specHasMirror := spec.Mirror != nil
	return serverHasMirror != specHasMirror
}

// keyValueMirrorToPrimaryFlip reports whether the desired transition is the
// DATA-PRESERVING mirror→primary promote: the server KV stream is a mirror and
// the (effective) spec drops the mirror to become a primary bucket. As with
// Streams, nats-server allows this conversion IN PLACE — the high-level
// UpdateKeyValue with a mirror-less config drops the mirror without deleting
// the underlying KV_<bucket> stream, so the replicated keys are RETAINED
// (verified against nats-server v2.14.0). The prior code routed this through
// js.DeleteKeyValue → CreateKeyValue, which dropped the entire backing stream
// (all keys) and recreated it empty — the KeyValue analog of the Stream
// data-loss bug.
func keyValueMirrorToPrimaryFlip(serverState *jetstream.StreamConfig, spec *api.KeyValueSpec) bool {
	if serverState == nil || spec == nil {
		return false
	}
	return serverState.Mirror != nil && spec.Mirror == nil
}

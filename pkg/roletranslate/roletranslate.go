// Package roletranslate is a TEST-SUPPORT / INTEGRATION-HARNESS API. It is
// NOT a general-purpose public API of nack.
//
// It exposes the smallest possible seam onto nack's real DRP local-role
// translation logic (internal/controller: shouldTranslateActiveRole /
// shouldTranslatePassiveRole, translateStreamSpecToPrimary /
// translateStreamSpecToMirror, and the StreamReconciler / KeyValueReconciler
// createOrUpdate reconcile-core that applies the result to the NATS server)
// so an external integration test — outside this Go module, where Go's
// internal/ import restriction otherwise applies — can drive the REAL
// production code path end to end:
//
//	stamp drp.cicada.io/local-role on a namespace
//	  -> build a StreamReconciler/KeyValueReconciler against a real
//	     JetStreamController
//	  -> call CreateOrUpdate on a scope-labeled CR
//	  -> observe the NATS server converge (mirror<->primary)
//
// Every type in this package is a direct alias of, or thin delegation to,
// the corresponding internal/controller type. Nothing here re-implements or
// copies the translation logic — it exists only so that logic is reachable
// from outside the module for test purposes. Keep this surface minimal:
// widen it only when a Phase B (or later) consumer needs another concrete
// seam, not speculatively.
package roletranslate

import (
	ctlr "github.com/nats-io/nack/internal/controller"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NatsConfig carries the NATS server connection settings for the
// JetStreamController built by NewController. Alias of controller.NatsConfig.
type NatsConfig = ctlr.NatsConfig

// Config carries the DRP-relevant controller behavior flags — at minimum
// Namespace, EnablePassiveRoleTranslation, CrossRegionNATSDomain, and
// MirrorRecreateOnConflict are relevant to driving role translation. Alias
// of controller.Config; see its doc comments for the full field set.
type Config = ctlr.Config

// JetStreamController is the NATS-facing seam StreamReconciler and
// KeyValueReconciler drive against. Alias of controller.JetStreamController.
type JetStreamController = ctlr.JetStreamController

// StreamReconciler is nack's real Stream reconciler. Construct it directly
// (StreamReconciler{Scheme: ..., JetStreamController: ...}) and call
// CreateOrUpdate to run the real local-role translation + apply logic
// against a Stream CR. Alias of controller.StreamReconciler.
type StreamReconciler = ctlr.StreamReconciler

// KeyValueReconciler is the KeyValue analog of StreamReconciler. Alias of
// controller.KeyValueReconciler.
type KeyValueReconciler = ctlr.KeyValueReconciler

// NewController builds the real JetStreamController used by nack's
// Stream/KeyValue reconcilers, pointed at the given NATS server and backed
// by the given Kubernetes client (a fake client is sufficient for an
// integration test — only the `nats` namespace's drp.cicada.io/local-role
// annotation and the scope-labeled CRs need to be present in it). Delegates
// directly to controller.NewJSController.
func NewController(k8sClient client.Client, nats *NatsConfig, cfg *Config) (JetStreamController, error) {
	return ctlr.NewJSController(k8sClient, nats, cfg)
}

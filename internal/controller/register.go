package controller

import (
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// The Config contains parameters to be considered by the registered controllers.
//
// ReadOnly prevents controllers from actually applying changes NATS resources.
//
// Namespace restricts the controller to resources of the given namespace.
type Config struct {
	ReadOnly               bool
	Namespace              string
	RequeueInterval        time.Duration
	CacheDir               string
	HealthProbeBindAddress string

	// EnableSiblingWebhook turns on the Stream/KeyValue sibling-conflict
	// admission webhook (control-loop mode only). When true, WebhookPort
	// and WebhookCertDir are honored.
	EnableSiblingWebhook bool
	WebhookPort          int
	WebhookCertDir       string

	// MirrorRecreateOnConflict, when true, makes the Stream and KeyValue
	// reconcilers force-delete the underlying NATS server stream and re-create
	// it from the K8s CR whenever the spec flips between source-mode and
	// mirror-mode (or whenever an UpdateConfiguration call returns one of the
	// mirror-incompatible NATS error codes: 10031 / 10034 / 10055).
	//
	// Defaults to false to preserve upstream behaviour.
	MirrorRecreateOnConflict bool

	// RequireBackupConfirmation gates the destructive recreate on an
	// external backup operator confirming that local data has been
	// captured. When true AND the local server stream has data AND the
	// cross-region peer can NOT be reached or does NOT have enough
	// messages, the controller sets a `BackupRequired=True` condition
	// on the CR and requeues without destroying. An external operator
	// (e.g. drp-operator) watches the condition, takes a backup, and
	// records the generation it confirmed via the
	// BackupConfirmedAnnotation. On the next reconcile, if the
	// annotation value matches the CR's current Generation, the
	// controller clears the condition and proceeds with the delete.
	//
	// Default false — preserves the v1 mirror-recreate behaviour.
	RequireBackupConfirmation bool

	// CrossRegionNATSURL is the NATS URL to use for the "is the peer
	// already holding this data?" pre-destruction probe. Typically the
	// other region's externally-reachable client port (e.g.
	// nats://nats-gw.ndev-2nd.mtrade.com.mx:4222 for west's view of
	// east). Empty disables the probe — when paired with
	// --require-backup-confirmation=true, that always demands an
	// external backup confirmation whenever local data is present.
	CrossRegionNATSURL string

	// CrossRegionNATSCredsPath is the local filesystem path inside the
	// controller container to the NATS credentials file used for the
	// cross-region probe. Typically mounted from a K8s Secret. Empty
	// disables auth — only useful for tests against a local
	// unauthenticated NATS server.
	CrossRegionNATSCredsPath string

	// BackupConfirmedAnnotation is the K8s annotation key the
	// reconciler reads to know that the configured external backup
	// operator has captured the local stream for the CR's current
	// Generation. Value must equal the CR's `metadata.generation` (as
	// a decimal string) to satisfy the gate. Default
	// "drp.cicada.io/backup-confirmed-generation".
	BackupConfirmedAnnotation string

	// DRPOperatorSA is the ServiceAccount username the drill-active
	// operator-only admission gate treats as the allowed writer on
	// scope-labeled CRs while a drill is in flight. Format:
	// `system:serviceaccount:<ns>:<sa>`. Empty falls back to
	// webhook.DefaultDRPOperatorServiceAccount.
	DRPOperatorSA string

	// DefaultAccount is the NATS account an UNLABELED Stream/KeyValue CR
	// resolves to in the account-aware sibling-conflict comparison. Chart
	// entries that omit `account` are the implicit default account; an
	// unlabeled CR (default account) must NOT collide with a labeled
	// non-default sibling sharing the same spec.name. Empty falls back to
	// webhook.DefaultNATSAccount ("JS").
	DefaultAccount string

	// EnablePassiveRoleTranslation, when true, makes the Stream and
	// KeyValue reconcilers consult the `drp.cicada.io/local-role`
	// annotation on the CR's namespace. When the annotation is
	// `passive`, the reconciler synthesizes a mirror config from
	// CrossRegionNATSDomain + spec.name (Stream) or KV_<bucket> (KV)
	// and applies that to the NATS server INSTEAD of the primary form
	// the CR may carry. The K8s CR is NOT modified — translation is
	// server-side only. Off by default.
	//
	// Pairs with CrossRegionNATSDomain: without a remote domain, the
	// controller can't synthesize the mirror externalApiPrefix, so
	// translation is skipped even when the annotation is present.
	EnablePassiveRoleTranslation bool

	// CrossRegionNATSDomain is the JetStream domain of the peer
	// region used to synthesize `externalApiPrefix=$JS.<domain>.API`
	// when EnablePassiveRoleTranslation fires. Example: when the
	// controller runs on dev-west with east as peer, the value is
	// "dev-2nd-east" — produced externalApiPrefix becomes
	// "$JS.dev-2nd-east.API". Empty disables translation regardless
	// of the namespace annotation.
	CrossRegionNATSDomain string
}

// RegisterAll registers all available jetStream controllers to the manager.
// natsCfg is specific to the nats server connection.
// controllerCfg defines behaviour of the registered controllers.
func RegisterAll(mgr ctrl.Manager, clientConfig *NatsConfig, config *Config) error {
	scheme := mgr.GetScheme()

	// Register controllers
	baseController, err := NewJSController(mgr.GetClient(), clientConfig, config)
	if err != nil {
		return fmt.Errorf("create base jetstream controller: %w", err)
	}

	if err := (&AccountReconciler{
		Scheme:              scheme,
		JetStreamController: baseController,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create account controller: %w", err)
	}

	if err := (&ConsumerReconciler{
		Scheme:              scheme,
		JetStreamController: baseController,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create consumer controller: %w", err)
	}

	if err := (&KeyValueReconciler{
		Scheme:              scheme,
		JetStreamController: baseController,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create key-value controller: %w", err)
	}

	if err := (&ObjectStoreReconciler{
		Scheme:              scheme,
		JetStreamController: baseController,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create object store controller: %w", err)
	}

	if err := (&StreamReconciler{
		Scheme:              scheme,
		JetStreamController: baseController,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create stream controller: %w", err)
	}

	return nil
}

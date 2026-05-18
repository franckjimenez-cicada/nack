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

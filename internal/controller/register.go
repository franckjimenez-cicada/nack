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

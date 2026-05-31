// Copyright 2020-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nats-io/nack/controllers/jetstream"
	"github.com/nats-io/nack/internal/controller"
	"github.com/nats-io/nack/internal/webhook"
	v1beta2 "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	clientset "github.com/nats-io/nack/pkg/jetstream/generated/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	BuildTime = "build-time-not-set"
	GitInfo   = "gitinfo-not-set"
	Version   = "not-set"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	klog.InitFlags(nil)
	// Opt into the new klog behavior so that -stderrthreshold is honored even
	// when -logtostderr=true (the default).
	// Ref: kubernetes/klog#212, kubernetes/klog#432
	_ = flag.Set("legacy_stderr_threshold_behavior", "false")
	_ = flag.Set("stderrthreshold", "INFO")

	// Explicitly register controller-runtime flags
	ctrl.RegisterFlags(nil)

	namespace := flag.String("namespace", v1.NamespaceAll, "Restrict to a namespace")
	version := flag.Bool("version", false, "Print the version and exit")
	creds := flag.String("creds", "", "NATS Credentials")
	nkey := flag.String("nkey", "", "NATS NKey")
	cert := flag.String("tlscert", "", "NATS TLS public certificate")
	key := flag.String("tlskey", "", "NATS TLS private key")
	ca := flag.String("tlsca", "", "NATS TLS certificate authority chain")
	tlsfirst := flag.Bool("tlsfirst", false, "If enabled, forces explicit TLS without waiting for Server INFO")
	server := flag.String("s", "", "NATS Server URL")
	crdConnect := flag.Bool("crd-connect", false, "If true, then NATS connections will be made from CRD config, not global config. Ignored if running with control loop, CRD options will always override global config")
	cleanupPeriod := flag.Duration("cleanup-period", 30*time.Second, "Period to run object cleanup")
	readOnly := flag.Bool("read-only", false, "Starts the controller without causing changes to the NATS resources")
	cacheDir := flag.String("cache-dir", "", "Directory to store cached credential and TLS files")
	controlLoop := flag.Bool("control-loop", false, "Experimental: Run controller with a full reconciliation control loop")
	controlLoopSyncInterval := flag.Duration("sync-interval", time.Minute, "Interval to perform scheduled reconcile")
	healthProbeBindAddress := flag.String("health-probe-bind-address", ":8081", "The address the probe endpoint binds to")
	enableWebhook := flag.Bool("enable-sibling-webhook", false, "Enable the Stream/KeyValue sibling-conflict admission webhook (control-loop mode only)")
	webhookPort := flag.Int("webhook-port", 9443, "Webhook server bind port")
	webhookCertDir := flag.String("webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "Webhook server TLS cert directory")
	mirrorRecreateOnConflict := flag.Bool("mirror-recreate-on-conflict", false, "Force-delete and re-create a Stream / KeyValue underlying NATS stream when the K8s spec flips between source-mode and mirror-mode, or when UpdateConfiguration returns a mirror-incompatible NATS error (10031 / 10034 / 10055). Off by default to preserve upstream semantics; opt in for DRP-style failover workflows where the only correct recovery is a clean recreate.")
	requireBackupConfirmation := flag.Bool("require-backup-confirmation", false, "Gate the destructive recreate triggered by --mirror-recreate-on-conflict on an external backup operator confirming local data via the annotation set by --backup-confirmed-annotation. When set and the local stream has data AND the cross-region peer is unreachable or holds fewer messages, the controller sets a BackupRequired=True condition and waits instead of destroying. Pairs with --cross-region-nats-url for the readiness probe.")
	crossRegionNATSURL := flag.String("cross-region-nats-url", "", "NATS URL used by --require-backup-confirmation to probe whether the peer region already holds this stream's data. Empty disables the probe — every destructive recreate against a stream with local data will demand external backup confirmation.")
	crossRegionNATSCredsPath := flag.String("cross-region-nats-creds-path", "", "Local filesystem path inside the controller container to the NATS credentials file used for the cross-region probe. Typically mounted from a K8s Secret. Empty disables auth.")
	backupConfirmedAnnotation := flag.String("backup-confirmed-annotation", "drp.cicada.io/backup-confirmed-generation", "Annotation key the controller reads to know an external backup operator has captured the CR's local state. Value must equal the CR's metadata.generation as a decimal string.")
	drpOperatorSA := flag.String("drp-operator-sa", webhook.DefaultDRPOperatorServiceAccount,
		"ServiceAccount username (format `system:serviceaccount:<ns>:<sa>`) allowed to mutate scope-labeled Stream/KeyValue CRs while the namespace carries the drill-active annotation. Defaults to the dev/stg convention; override for prod or other environments where drp-operator runs under a different namespace/SA.")
	selfServiceAccount := flag.String("self-service-account", "",
		"ServiceAccount username (format `system:serviceaccount:<ns>:<sa>`) of nack's OWN controller, ALWAYS exempt from the drill-active operator-only gate so the controller can manage its own CRs/finalizers even mid-drill (prevents the finalizer-removal deadlock that leaves CRs stuck Terminating during a promote). Empty (default) auto-detects: the conventional SA name `jetstream-controller` under the namespace from the POD_NAMESPACE downward-API env, falling back to `system:serviceaccount:nats:jetstream-controller`. Set explicitly only if your cluster renames the controller SA.")
	defaultAccount := flag.String("default-account", webhook.DefaultNATSAccount,
		"NATS account name an UNLABELED Stream/KeyValue CR resolves to in the account-aware sibling-conflict webhook. Chart entries that omit `account` are the implicit default account; with this flag an unlabeled CR (e.g. a JS stream) no longer collides with a labeled non-default sibling (e.g. its nats-qa twin sharing the same spec.name). Defaults to the canonical default account.")
	enablePassiveRoleTranslation := flag.Bool("enable-passive-role-translation", false,
		"Translate primary-form Stream/KeyValue CRs to mirror form when the namespace carries `drp.cicada.io/local-role=passive`. The K8s CR is NOT modified — translation only affects what is applied to the NATS server. Eliminates the ArgoCD selfHeal vs drp-operator race on the passive region post-flip. Requires --cross-region-nats-domain to be set; off by default.")
	crossRegionNATSDomain := flag.String("cross-region-nats-domain", "",
		"JetStream domain of the peer region (e.g. `dev-2nd-east` when running on dev-west). Used by --enable-passive-role-translation to synthesize `externalApiPrefix=$JS.<domain>.API` on the translated mirror config. Required for translation; ignored when --enable-passive-role-translation is off.")

	flag.Parse()

	if *version {
		fmt.Printf("%s version %s (%s), built %s\n", os.Args[0], Version, GitInfo, BuildTime)
		return nil
	}

	if *server == "" && !*crdConnect {
		return errors.New("NATS Server URL is required")
	}

	config, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("get kubernetes rest config: %w", err)
	}

	if *controlLoop {
		klog.Warning("Starting JetStream controller in experimental control loop mode")

		natsCfg := &controller.NatsConfig{
			ClientName:  "jetstream-controller",
			Credentials: *creds,
			NKey:        *nkey,
			ServerURL:   *server,
			CAs:         []string{},
			Certificate: *cert,
			Key:         *key,
			TLSFirst:    *tlsfirst,
		}

		if *ca != "" {
			natsCfg.CAs = []string{*ca}
		}

		controllerCfg := &controller.Config{
			ReadOnly:                     *readOnly,
			Namespace:                    *namespace,
			CacheDir:                     *cacheDir,
			RequeueInterval:              *controlLoopSyncInterval,
			HealthProbeBindAddress:       *healthProbeBindAddress,
			EnableSiblingWebhook:         *enableWebhook,
			WebhookPort:                  *webhookPort,
			WebhookCertDir:               *webhookCertDir,
			MirrorRecreateOnConflict:     *mirrorRecreateOnConflict,
			RequireBackupConfirmation:    *requireBackupConfirmation,
			CrossRegionNATSURL:           *crossRegionNATSURL,
			CrossRegionNATSCredsPath:     *crossRegionNATSCredsPath,
			BackupConfirmedAnnotation:    *backupConfirmedAnnotation,
			DRPOperatorSA:                *drpOperatorSA,
			ControllerSelfSA:             webhook.ResolveControllerServiceAccount(*selfServiceAccount, os.Getenv),
			DefaultAccount:               *defaultAccount,
			EnablePassiveRoleTranslation: *enablePassiveRoleTranslation,
			CrossRegionNATSDomain:        *crossRegionNATSDomain,
		}

		return runControlLoop(config, natsCfg, controllerCfg)
	}

	// K8S API Client.
	kc, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	// JetStream CRDs client.
	jc, err := clientset.NewForConfig(config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := jetstream.NewController(jetstream.Options{
		// FIXME: Move context to be param from Run
		// to avoid keeping state in options.
		Ctx:                      ctx,
		NATSCredentials:          *creds,
		NATSNKey:                 *nkey,
		NATSServerURL:            *server,
		NATSCA:                   *ca,
		NATSCertificate:          *cert,
		NATSKey:                  *key,
		NATSTLSFirst:             *tlsfirst,
		KubeIface:                kc,
		JetstreamIface:           jc,
		Namespace:                *namespace,
		CRDConnect:               *crdConnect,
		CleanupPeriod:            *cleanupPeriod,
		ReadOnly:                 *readOnly,
		MirrorRecreateOnConflict: *mirrorRecreateOnConflict,
	})

	klog.Infof("Starting %s v%s...", os.Args[0], Version)
	klog.Infof("Running in LEGACY mode")
	if *readOnly {
		klog.Infof("Running in read-only mode: JetStream state in server will not be changed")
	}
	go handleSignals(cancel)
	return ctrl.Run()
}

func runControlLoop(config *rest.Config, natsCfg *controller.NatsConfig, controllerCfg *controller.Config) error {
	// Setup scheme
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1beta2.AddToScheme(scheme))

	log.SetLogger(klog.NewKlogr())

	ctrlOpts := ctrl.Options{
		Scheme:                 scheme,
		Logger:                 log.Log,
		HealthProbeBindAddress: controllerCfg.HealthProbeBindAddress,
	}

	if controllerCfg.EnableSiblingWebhook {
		ctrlOpts.WebhookServer = ctrlwebhook.NewServer(ctrlwebhook.Options{
			Port:    controllerCfg.WebhookPort,
			CertDir: controllerCfg.WebhookCertDir,
		})
	}

	if controllerCfg.Namespace != "" {
		ctrlOpts.Cache = cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				controllerCfg.Namespace: {},
			},
		}
		// The drill-active operator-only gate reads
		// corev1.Namespace objects to check the drill-active
		// annotation. Namespaces are cluster-scoped, so the
		// DefaultNamespaces scoping above would prevent them from
		// being cached / watched — every admission call would
		// fall back to a direct apiserver Get. Pin Namespace via
		// ByObject with an empty Namespaces map to keep it
		// cluster-wide-cached even when the rest of the manager
		// is namespace-scoped.
		ctrlOpts.Cache.ByObject = map[ctrlclient.Object]cache.ByObject{
			&corev1.Namespace{}: {},
		}
	}
	// When controllerCfg.Namespace is empty the manager already caches
	// every resource cluster-wide (including Namespace), so no extra
	// wiring is needed here — the gate's Get hits the cache directly.

	mgr, err := ctrl.NewManager(config, ctrlOpts)
	if err != nil {
		return fmt.Errorf("unable to start manager: %w", err)
	}

	if controllerCfg.CacheDir == "" {
		cacheDir, err := os.MkdirTemp(".", "nack")
		if err != nil {
			return fmt.Errorf("create cache dir: %w", err)
		}
		defer os.RemoveAll(cacheDir)
		cacheDir, err = filepath.Abs(cacheDir)
		if err != nil {
			return fmt.Errorf("get absolute cache dir: %w", err)
		}
		controllerCfg.CacheDir = cacheDir
	} else {
		if _, err := os.Stat(controllerCfg.CacheDir); os.IsNotExist(err) {
			err = os.MkdirAll(controllerCfg.CacheDir, 0o755)
			if err != nil {
				return fmt.Errorf("create cache dir: %w", err)
			}
		}
	}

	err = controller.RegisterAll(mgr, natsCfg, controllerCfg)
	if err != nil {
		return fmt.Errorf("register jetstream controllers: %w", err)
	}

	if controllerCfg.EnableSiblingWebhook {
		if err := webhook.SetupWithManager(mgr, webhook.Options{
			DRPOperatorSA:    controllerCfg.DRPOperatorSA,
			ControllerSelfSA: controllerCfg.ControllerSelfSA,
			DefaultAccount:   controllerCfg.DefaultAccount,
		}); err != nil {
			return fmt.Errorf("register sibling-conflict webhook: %w", err)
		}
		klog.Infof("sibling-conflict admission webhook enabled (drp-operator SA: %q, controller self SA: %q)",
			controllerCfg.DRPOperatorSA, controllerCfg.ControllerSelfSA)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	klog.Info("starting manager")
	klog.Infof("Running in CONTROL-LOOP mode")
	if controllerCfg.ReadOnly {
		klog.Infof("Running in read-only mode: JetStream state in server will not be changed")
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}

func handleSignals(cancel context.CancelFunc) {
	sigc := make(chan os.Signal, 2)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)

	for sig := range sigc {
		switch sig {
		case syscall.SIGINT:
			os.Exit(130)
		case syscall.SIGTERM:
			cancel()
			return
		}
	}
}

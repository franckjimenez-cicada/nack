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

package webhook

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

// Options is the parameter bag for SetupWithManager. Kept as a struct so
// future per-webhook knobs (e.g. additional bypass SAs, opt-in dry-run
// mode) can be added without changing the function signature.
type Options struct {
	// DRPOperatorSA is the ServiceAccount username allowed to mutate
	// scope-labeled CRs while a drill-active annotation is set on the
	// namespace. Format: `system:serviceaccount:<ns>:<sa>`. Empty falls
	// back to DefaultDRPOperatorServiceAccount.
	DRPOperatorSA string
}

// SetupWithManager registers the Stream + KeyValue sibling-conflict validators
// on mgr's webhook server. The webhook server bind address / certs are
// configured upstream via ctrl.Options.WebhookServer.
//
// Paths:
//   - /validate-jetstream-nats-io-v1beta2-stream
//   - /validate-jetstream-nats-io-v1beta2-keyvalue
//
// These paths must match the clientConfig.service.path in the
// ValidatingWebhookConfiguration manifest (see deploy/webhook.yml).
func SetupWithManager(mgr ctrl.Manager, opts Options) error {
	scheme := mgr.GetScheme()
	srv := mgr.GetWebhookServer()
	if srv == nil {
		return fmt.Errorf("manager has no webhook server configured")
	}

	srv.Register(
		"/validate-jetstream-nats-io-v1beta2-stream",
		admission.WithValidator[*api.Stream](scheme, &StreamValidator{
			Client:        mgr.GetClient(),
			DRPOperatorSA: opts.DRPOperatorSA,
		}),
	)
	srv.Register(
		"/validate-jetstream-nats-io-v1beta2-keyvalue",
		admission.WithValidator[*api.KeyValue](scheme, &KeyValueValidator{
			Client:        mgr.GetClient(),
			DRPOperatorSA: opts.DRPOperatorSA,
		}),
	)

	return nil
}

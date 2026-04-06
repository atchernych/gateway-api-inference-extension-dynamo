/*
Copyright 2025 The Kubernetes Authors.

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

package runner

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	epplight "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light/server"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1.Install(scheme))
}

// Runner encapsulates the light EPP server lifecycle.
// External projects can import this, swap in a custom EndpointPicker via
// WithPicker(), and call Run().
type Runner struct {
	picker epplight.EndpointPicker
}

// NewRunner creates a Runner with the default RandomPicker.
func NewRunner() *Runner {
	return &Runner{
		picker: epplight.NewRandomPicker(),
	}
}

// WithPicker sets a custom EndpointPicker implementation.
func (r *Runner) WithPicker(picker epplight.EndpointPicker) *Runner {
	r.picker = picker
	return r
}

// Run parses flags, creates the controller manager, wires everything together,
// and blocks until the context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	opts := server.NewOptions()
	opts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	ctrl.SetLogger(zap.New())
	logger := ctrl.Log.WithName("epp-light")

	if err := opts.Validate(); err != nil {
		return fmt.Errorf("invalid options: %w", err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		return fmt.Errorf("unable to create controller manager: %w", err)
	}

	datastore := epplight.NewDatastore()

	serverRunner := &server.ExtProcServerRunner{
		GRPCPort:      opts.GRPCPort,
		PoolNamespace: opts.PoolNamespace,
		PoolName:      opts.PoolName,
		Datastore:     datastore,
		Picker:        r.picker,
		SecureServing: opts.SecureServing,
		CertPath:      opts.CertPath,
	}

	if err := serverRunner.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup controllers: %w", err)
	}

	if err := mgr.Add(serverRunner.AsRunnable(logger)); err != nil {
		return fmt.Errorf("failed to add ext-proc server runnable: %w", err)
	}

	logger.Info(fmt.Sprintf("Starting light EPP for pool %s/%s on port %d",
		opts.PoolNamespace, opts.PoolName, opts.GRPCPort))

	return mgr.Start(ctx)
}

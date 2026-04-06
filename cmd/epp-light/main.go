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

// The epp-light binary is a minimal, reference Endpoint Picker (EPP) implementation
// that demonstrates the Endpoint Picker Protocol using a simple random selection strategy.
//
// To use a custom endpoint picker, implement the epplight.EndpointPicker interface and
// replace NewRandomPicker() below with your implementation.
package main

import (
	"fmt"
	"os"

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

func main() {
	opts := server.NewOptions()
	opts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	ctrl.SetLogger(zap.New())
	logger := ctrl.Log.WithName("epp-light")

	if err := opts.Validate(); err != nil {
		logger.Error(err, "Invalid options")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		logger.Error(err, "Unable to create controller manager")
		os.Exit(1)
	}

	datastore := epplight.NewDatastore()
	picker := epplight.NewRandomPicker()

	runner := &server.ExtProcServerRunner{
		GRPCPort:      opts.GRPCPort,
		PoolNamespace: opts.PoolNamespace,
		PoolName:      opts.PoolName,
		Datastore:     datastore,
		Picker:        picker,
		SecureServing: opts.SecureServing,
		CertPath:      opts.CertPath,
	}

	if err := runner.SetupWithManager(mgr); err != nil {
		logger.Error(err, "Failed to setup controllers")
		os.Exit(1)
	}

	if err := mgr.Add(runner.AsRunnable(logger)); err != nil {
		logger.Error(err, "Failed to add ext-proc server runnable")
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("Starting light EPP for pool %s/%s on port %d", opts.PoolNamespace, opts.PoolName, opts.GRPCPort))
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		logger.Error(err, "Manager exited with error")
		os.Exit(1)
	}
}

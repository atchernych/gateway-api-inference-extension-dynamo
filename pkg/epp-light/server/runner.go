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

package server

import (
	"context"
	"crypto/tls"
	"fmt"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/gateway-api-inference-extension/internal/runnable"
	tlsutil "sigs.k8s.io/gateway-api-inference-extension/internal/tls"
	epplight "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light/controller"
)

// ExtProcServerRunner manages the lifecycle of the light EPP ext-proc server.
type ExtProcServerRunner struct {
	GRPCPort      int
	PoolNamespace string
	PoolName      string
	Datastore     epplight.Datastore
	Picker        epplight.EndpointPicker
	SecureServing bool
	CertPath      string
}

// SetupWithManager registers the InferencePool and Pod reconcilers with the controller manager.
func (r *ExtProcServerRunner) SetupWithManager(mgr ctrl.Manager) error {
	if err := (&controller.InferencePoolReconciler{
		Reader:        mgr.GetClient(),
		Datastore:     r.Datastore,
		PoolNamespace: r.PoolNamespace,
		PoolName:      r.PoolName,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed setting up InferencePoolReconciler: %w", err)
	}

	if err := (&controller.PodReconciler{
		Reader:    mgr.GetClient(),
		Datastore: r.Datastore,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed setting up PodReconciler: %w", err)
	}

	return nil
}

// AsRunnable returns a manager.Runnable that starts the ext-proc gRPC server.
func (r *ExtProcServerRunner) AsRunnable(logger logr.Logger) manager.Runnable {
	return runnable.NoLeaderElection(manager.RunnableFunc(func(ctx context.Context) error {
		var srv *grpc.Server
		if r.SecureServing {
			cert, err := r.loadOrCreateCert(logger)
			if err != nil {
				return err
			}
			creds := credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{"h2"},
			})
			srv = grpc.NewServer(grpc.Creds(creds))
		} else {
			srv = grpc.NewServer()
		}

		extProcServer := epplight.NewStreamingServer(r.Datastore, r.Picker)
		extProcPb.RegisterExternalProcessorServer(srv, extProcServer)

		// Register health check service.
		healthcheck := health.NewServer()
		healthgrpc.RegisterHealthServer(srv, healthcheck)
		svcName := extProcPb.ExternalProcessor_ServiceDesc.ServiceName
		logger.Info("Setting ExternalProcessor service status to SERVING", "serviceName", svcName)
		healthcheck.SetServingStatus(svcName, healthgrpc.HealthCheckResponse_SERVING)

		return runnable.GRPCServer("epp-light", srv, r.GRPCPort).Start(ctx)
	}))
}

func (r *ExtProcServerRunner) loadOrCreateCert(logger logr.Logger) (tls.Certificate, error) {
	if r.CertPath != "" {
		return tls.LoadX509KeyPair(r.CertPath+"/tls.crt", r.CertPath+"/tls.key")
	}
	return tlsutil.CreateSelfSignedTLSCertificate(logger)
}

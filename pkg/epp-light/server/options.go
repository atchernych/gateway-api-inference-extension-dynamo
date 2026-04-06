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
	"errors"

	"github.com/spf13/pflag"
)

const (
	DefaultGRPCPort      = 9002
	DefaultPoolNamespace = "default"
)

// Options contains the minimal CLI configuration for the light EPP.
type Options struct {
	GRPCPort      int
	PoolNamespace string
	PoolName      string
	SecureServing bool
	CertPath      string
}

// NewOptions returns Options with default values.
func NewOptions() *Options {
	return &Options{
		GRPCPort:      DefaultGRPCPort,
		SecureServing: false,
	}
}

// AddFlags registers CLI flags for the light EPP options.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fs.IntVar(&o.GRPCPort, "grpc-port", o.GRPCPort, "gRPC port for ext-proc communication with the proxy.")
	fs.StringVar(&o.PoolNamespace, "pool-namespace", o.PoolNamespace, "Namespace of the InferencePool.")
	fs.StringVar(&o.PoolName, "pool-name", o.PoolName, "Name of the InferencePool.")
	fs.BoolVar(&o.SecureServing, "secure-serving", o.SecureServing, "Enables TLS for the gRPC server.")
	fs.StringVar(&o.CertPath, "cert-path", o.CertPath, "Path to TLS certificate (tls.crt and tls.key).")
}

// Validate checks that required options are set.
func (o *Options) Validate() error {
	if o.PoolName == "" {
		return errors.New("--pool-name is required")
	}
	if o.PoolNamespace == "" {
		o.PoolNamespace = DefaultPoolNamespace
	}
	return nil
}

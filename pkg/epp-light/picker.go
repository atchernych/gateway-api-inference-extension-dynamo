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

// Package epplight provides a minimal, API-focused Endpoint Picker (EPP) implementation
// for the Gateway API Inference Extension. It implements the Endpoint Picker Protocol
// (proposal 004) and watches the InferencePool CRD for pod discovery.
//
// The core abstraction is the EndpointPicker interface, which allows anyone to implement
// their own endpoint selection logic while the package handles all ext-proc protocol details.
package epplight

import "context"

// Endpoint represents a backend endpoint available in the InferencePool.
type Endpoint struct {
	// Address is the pod IP address.
	Address string
	// Port is the target port number as a string.
	Port string
	// Name is the pod name (namespace/name format).
	Name string
	// Labels are the pod's Kubernetes labels.
	Labels map[string]string
}

// RequestInfo contains information about the incoming request that the picker
// may use to make its selection decision.
type RequestInfo struct {
	// Headers from the incoming HTTP request.
	Headers map[string]string
	// Body is the raw request body bytes. May be nil for header-only requests (e.g., GET).
	Body []byte
	// Model is the model name extracted from the request body, if present.
	Model string
	// CandidateSubset is the list of candidate endpoints from the
	// x-gateway-destination-endpoint-subset metadata. When non-empty,
	// the picker MUST select only from endpoints matching this set.
	// When empty, all pool endpoints are candidates.
	CandidateSubset []string
}

// PickResult is the output of endpoint selection.
type PickResult struct {
	// Endpoint is the primary selected endpoint in "ip:port" format.
	Endpoint string
	// Fallbacks is an optional ordered list of fallback endpoints in "ip:port" format.
	// If the primary endpoint is unavailable, the proxy may try these in order.
	Fallbacks []string
}

// EndpointPicker is the core interface for endpoint selection.
// Implement this interface to build a custom EPP with your own selection logic.
//
// The server handles all Envoy ext-proc protocol details — subset filtering,
// metadata generation, header/body forwarding — so implementations only need
// to decide which endpoint to route to.
type EndpointPicker interface {
	// Pick selects one or more endpoints from the available set.
	// The endpoints slice contains the current set of ready endpoints,
	// already filtered by CandidateSubset if applicable.
	Pick(ctx context.Context, req *RequestInfo, endpoints []Endpoint) (*PickResult, error)
}

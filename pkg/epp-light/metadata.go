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

package epplight

// Endpoint Picker Protocol constants per proposal 004.
// See: docs/proposals/004-endpoint-picker-protocol/
const (
	// SubsetFilterNamespace is the metadata namespace for the candidate endpoint subset.
	SubsetFilterNamespace = "envoy.lb.subset_hint"
	// SubsetFilterKey is the metadata key for the candidate endpoint subset list.
	SubsetFilterKey = "x-gateway-destination-endpoint-subset"
	// DestinationEndpointNamespace is the metadata namespace for the selected endpoint.
	DestinationEndpointNamespace = "envoy.lb"
	// DestinationEndpointKey is the header and metadata key for the selected endpoint.
	DestinationEndpointKey = "x-gateway-destination-endpoint"
)

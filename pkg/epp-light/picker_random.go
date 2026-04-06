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

import (
	"context"
	"fmt"
	"math/rand"
	"net"
)

// RandomPicker is the default EndpointPicker implementation that selects
// a random endpoint from the available set. It serves as a reference
// implementation for the EndpointPicker interface.
type RandomPicker struct{}

// NewRandomPicker creates a new RandomPicker.
func NewRandomPicker() *RandomPicker {
	return &RandomPicker{}
}

// Pick selects a random endpoint from the available endpoints.
func (p *RandomPicker) Pick(_ context.Context, _ *RequestInfo, endpoints []Endpoint) (*PickResult, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	ep := endpoints[rand.Intn(len(endpoints))]
	return &PickResult{
		Endpoint: net.JoinHostPort(ep.Address, ep.Port),
	}, nil
}

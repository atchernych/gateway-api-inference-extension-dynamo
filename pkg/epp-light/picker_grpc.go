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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light/proto/gen"
)

// GRPCPicker implements EndpointPicker by calling a remote gRPC EndpointPickerService.
// This allows non-Go implementations (Rust, Python, C++, etc.) to provide custom
// endpoint selection logic over the network.
type GRPCPicker struct {
	client pb.EndpointPickerServiceClient
	conn   *grpc.ClientConn
}

// NewGRPCPicker creates a GRPCPicker that connects to the given address.
func NewGRPCPicker(address string) (*GRPCPicker, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to picker service at %s: %w", address, err)
	}
	return &GRPCPicker{
		client: pb.NewEndpointPickerServiceClient(conn),
		conn:   conn,
	}, nil
}

// Pick calls the remote EndpointPickerService to select an endpoint.
func (p *GRPCPicker) Pick(ctx context.Context, req *RequestInfo, endpoints []Endpoint) (*PickResult, error) {
	protoReq := &pb.PickRequest{
		Headers:         req.Headers,
		Body:            req.Body,
		Model:           req.Model,
		CandidateSubset: req.CandidateSubset,
		Endpoints:       toProtoEndpoints(endpoints),
	}

	resp, err := p.client.Pick(ctx, protoReq)
	if err != nil {
		return nil, fmt.Errorf("remote picker failed: %w", err)
	}

	return &PickResult{
		Endpoint:  resp.Endpoint,
		Fallbacks: resp.Fallbacks,
	}, nil
}

// Close closes the underlying gRPC connection.
func (p *GRPCPicker) Close() error {
	return p.conn.Close()
}

func toProtoEndpoints(endpoints []Endpoint) []*pb.EndpointInfo {
	result := make([]*pb.EndpointInfo, len(endpoints))
	for i, ep := range endpoints {
		result[i] = &pb.EndpointInfo{
			Address: ep.Address,
			Port:    ep.Port,
			Name:    ep.Name,
			Labels:  ep.Labels,
		}
	}
	return result
}

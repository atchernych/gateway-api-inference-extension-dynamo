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
	"encoding/json"
	"net"
	"strconv"
	"strings"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/util/sets"

	envoy "sigs.k8s.io/gateway-api-inference-extension/pkg/common/envoy"
)

// systemOwnedHeaders are headers managed by the EPP or proxy layer that should
// not be forwarded to the backend.
var systemOwnedHeaders = sets.New(
	strings.ToLower(DestinationEndpointKey),
	strings.ToLower(SubsetFilterKey),
	"content-length",
)

func isSystemOwnedHeader(key string) bool {
	return systemOwnedHeaders.Has(strings.ToLower(key))
}

func (s *StreamingServer) handleRequestHeaders(reqCtx *requestContext, req *extProcPb.ProcessingRequest_RequestHeaders) {
	for _, header := range req.RequestHeaders.Headers.Headers {
		reqCtx.request.Headers[header.Key] = envoy.GetHeaderValue(header)
	}
}

func (s *StreamingServer) generateRequestHeaderResponse(ctx context.Context, reqCtx *requestContext) *extProcPb.ProcessingResponse {
	dynamicMetadata := generateDestinationMetadata(reqCtx.targetEndpoint)
	return &extProcPb.ProcessingResponse{
		Response: &extProcPb.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extProcPb.HeadersResponse{
				Response: &extProcPb.CommonResponse{
					ClearRouteCache: true,
					HeaderMutation: &extProcPb.HeaderMutation{
						SetHeaders: generateRequestHeaders(ctx, reqCtx),
					},
				},
			},
		},
		DynamicMetadata: dynamicMetadata,
	}
}

func generateRequestHeaders(ctx context.Context, reqCtx *requestContext) []*configPb.HeaderValueOption {
	headers := []*configPb.HeaderValueOption{
		{
			Header: &configPb.HeaderValue{
				Key:      DestinationEndpointKey,
				RawValue: []byte(reqCtx.targetEndpoint),
			},
		},
	}

	if reqCtx.requestSize > 0 {
		headers = append(headers, &configPb.HeaderValueOption{
			Header: &configPb.HeaderValue{
				Key:      "Content-Length",
				RawValue: []byte(strconv.Itoa(reqCtx.requestSize)),
			},
		})
	}

	// Propagate trace context headers.
	traceHeaders := make(map[string]string)
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.MapCarrier(traceHeaders))
	for key, value := range traceHeaders {
		headers = append(headers, &configPb.HeaderValueOption{
			Header: &configPb.HeaderValue{
				Key:      key,
				RawValue: []byte(value),
			},
		})
	}

	// Forward non-system-owned headers.
	for key, value := range reqCtx.request.Headers {
		if isSystemOwnedHeader(key) {
			continue
		}
		headers = append(headers, &configPb.HeaderValueOption{
			Header: &configPb.HeaderValue{
				Key:      key,
				RawValue: []byte(value),
			},
		})
	}
	return headers
}

// generateDestinationMetadata creates the envoy.lb dynamic metadata with the selected endpoint.
func generateDestinationMetadata(endpoint string) *structpb.Struct {
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			DestinationEndpointNamespace: {
				Kind: &structpb.Value_StructValue{
					StructValue: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							DestinationEndpointKey: {
								Kind: &structpb.Value_StringValue{
									StringValue: endpoint,
								},
							},
						},
					},
				},
			},
		},
	}
}

// extractModelFromBody extracts the "model" field from a JSON request body.
// Returns empty string if the body is not JSON or has no "model" field.
func extractModelFromBody(body []byte) string {
	var bodyMap map[string]any
	if err := json.Unmarshal(body, &bodyMap); err != nil {
		return ""
	}
	model, _ := bodyMap["model"].(string)
	return model
}

// extractCandidateSubset extracts the candidate endpoint subset from the request metadata.
// Returns nil if no subset filter is present.
func extractCandidateSubset(metadata map[string]any) []string {
	if metadata == nil {
		return nil
	}
	subsetMap, ok := metadata[SubsetFilterNamespace].(map[string]any)
	if !ok {
		return nil
	}
	endpointList, ok := subsetMap[SubsetFilterKey].([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(endpointList))
	for _, ep := range endpointList {
		if s, ok := ep.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

// filterEndpointsBySubset filters endpoints to only those matching the candidate subset.
// The subset contains entries in "ip:port" format; we match by IP address.
func filterEndpointsBySubset(endpoints []Endpoint, subset []string) []Endpoint {
	if len(subset) == 0 {
		return nil
	}
	allowedIPs := sets.New[string]()
	for _, ep := range subset {
		host, _, err := net.SplitHostPort(ep)
		if err != nil {
			allowedIPs.Insert(ep) // Treat as bare IP if not ip:port.
		} else {
			allowedIPs.Insert(host)
		}
	}
	var filtered []Endpoint
	for _, ep := range endpoints {
		if allowedIPs.Has(ep.Address) {
			filtered = append(filtered, ep)
		}
	}
	return filtered
}

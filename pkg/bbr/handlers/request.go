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

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	basepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	eppb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/bbr/metrics"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const modelHeader = "X-Gateway-Model-Name"

// Dynamo-related
const (
	workerIDHeader   = "x-worker-instance-id"
	injectHintHeader = "x-epp-inject-nvext-worker-instance-id"
)

// HandleRequestBody handles request bodies.
func (s *Server) HandleRequestBody(ctx context.Context, data map[string]any) ([]*eppb.ProcessingResponse, error) {
	logger := log.FromContext(ctx)
	var ret []*eppb.ProcessingResponse

	// If we captured a worker id hint in the headers phase, inject it into body JSON:
	// nvext.backend_instance_id = <workerID>
	if wid := strings.TrimSpace(s.workerIDHint); wid != "" {
		// ensure nvext is a map[string]any
		if nv, ok := data["nvext"]; !ok || nv == nil {
			data["nvext"] = map[string]any{"backend_instance_id": wid}
		} else if m, ok := nv.(map[string]any); ok {
			m["backend_instance_id"] = wid
		} else {
			// if nvext was some other type, replace with a clean map
			data["nvext"] = map[string]any{"backend_instance_id": wid}
		}
	}

	requestBodyBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	modelVal, ok := data["model"]
	if !ok {
		metrics.RecordModelNotInBodyCounter()
		logger.V(logutil.DEFAULT).Info("Request body does not contain model parameter")
		if s.streaming {
			// still stream the possibly mutated body
			ret = append(ret, &eppb.ProcessingResponse{
				Response: &eppb.ProcessingResponse_RequestHeaders{
					RequestHeaders: &eppb.HeadersResponse{},
				},
			})
			ret = addStreamedBodyResponse(ret, requestBodyBytes)
			return ret, nil
		}

		// non-streaming: return a body response with the (possibly) mutated body
		return []*eppb.ProcessingResponse{
			{
				Response: &eppb.ProcessingResponse_RequestBody{
					RequestBody: &eppb.BodyResponse{
						Response: &eppb.CommonResponse{
							BodyMutation: &eppb.BodyMutation{
								Mutation: &eppb.BodyMutation_Body{
									Body: requestBodyBytes,
								},
							},
						},
					},
				},
			},
		}, nil
	}

	modelStr, ok := modelVal.(string)
	if !ok {
		metrics.RecordModelNotParsedCounter()
		logger.V(logutil.DEFAULT).Info("Model parameter value is not a string")
		return nil, fmt.Errorf("the model parameter value %v is not a string", modelVal)
	}

	metrics.RecordSuccessCounter()

	if s.streaming {
		// set the model header, then stream the (possibly) mutated body
		ret = append(ret, &eppb.ProcessingResponse{
			Response: &eppb.ProcessingResponse_RequestHeaders{
				RequestHeaders: &eppb.HeadersResponse{
					Response: &eppb.CommonResponse{
						ClearRouteCache: true,
						HeaderMutation: &eppb.HeaderMutation{
							SetHeaders: []*basepb.HeaderValueOption{
								{
									Header: &basepb.HeaderValue{
										Key:      modelHeader,
										RawValue: []byte(modelStr),
									},
								},
								// also keep the worker id header if we have one
								func() *basepb.HeaderValueOption {
									if strings.TrimSpace(s.workerIDHint) == "" {
										return nil
									}
									return &basepb.HeaderValueOption{
										Header: &basepb.HeaderValue{
											Key:      workerIDHeader,
											RawValue: []byte(s.workerIDHint),
										},
									}
								}(),
							},
						},
					},
				},
			},
		})

		// prune nil entries if worker id not present
		hm := ret[len(ret)-1].GetRequestHeaders().GetResponse().GetHeaderMutation()
		if hm != nil && hm.SetHeaders != nil {
			out := hm.SetHeaders[:0]
			for _, h := range hm.SetHeaders {
				if h != nil {
					out = append(out, h)
				}
			}
			hm.SetHeaders = out
		}

		ret = addStreamedBodyResponse(ret, requestBodyBytes)
		return ret, nil
	}

	// Non-streaming: set model header and replace the body with our mutated JSON
	return []*eppb.ProcessingResponse{
		{
			Response: &eppb.ProcessingResponse_RequestBody{
				RequestBody: &eppb.BodyResponse{
					Response: &eppb.CommonResponse{
						// Necessary so that the new headers are used in the routing decision.
						ClearRouteCache: true,
						HeaderMutation: &eppb.HeaderMutation{
							SetHeaders: []*basepb.HeaderValueOption{
								{
									Header: &basepb.HeaderValue{
										Key:      modelHeader,
										RawValue: []byte(modelStr),
									},
								},
								func() *basepb.HeaderValueOption {
									if strings.TrimSpace(s.workerIDHint) == "" {
										return nil
									}
									return &basepb.HeaderValueOption{
										Header: &basepb.HeaderValue{
											Key:      workerIDHeader,
											RawValue: []byte(s.workerIDHint),
										},
									}
								}(),
							},
						},
						BodyMutation: &eppb.BodyMutation{
							Mutation: &eppb.BodyMutation_Body{
								Body: requestBodyBytes,
							},
						},
					},
				},
			},
		},
	}, nil
}

func addStreamedBodyResponse(responses []*eppb.ProcessingResponse, requestBodyBytes []byte) []*eppb.ProcessingResponse {
	return append(responses, &extProcPb.ProcessingResponse{
		Response: &extProcPb.ProcessingResponse_RequestBody{
			RequestBody: &extProcPb.BodyResponse{
				Response: &extProcPb.CommonResponse{
					BodyMutation: &extProcPb.BodyMutation{
						Mutation: &extProcPb.BodyMutation_StreamedResponse{
							StreamedResponse: &extProcPb.StreamedBodyResponse{
								Body:        requestBodyBytes,
								EndOfStream: true,
							},
						},
					},
				},
			},
		},
	})
}

// HandleRequestHeaders handles request headers.
func (s *Server) HandleRequestHeaders(headers *eppb.HttpHeaders) ([]*eppb.ProcessingResponse, error) {
	// Look for our hint header and/or direct worker id header.
	s.workerIDHint = "" // reset per request
	if m := headers.GetHeaders(); m != nil {
		for _, h := range m.GetHeaders() {
			k := strings.ToLower(h.GetKey())
			if k == injectHintHeader || k == workerIDHeader {
				if rv := h.GetRawValue(); len(rv) > 0 {
					s.workerIDHint = strings.TrimSpace(string(rv))
				} else {
					s.workerIDHint = strings.TrimSpace(h.GetValue())
				}
				// prefer the inject hint if both are present; break on the hint
				if k == injectHintHeader {
					break
				}
			}
		}
	}

	// No header mutations needed here; body phase will do the JSON injection.
	return []*eppb.ProcessingResponse{
		{
			Response: &eppb.ProcessingResponse_RequestHeaders{
				RequestHeaders: &eppb.HeadersResponse{},
			},
		},
	}, nil
}

// HandleRequestTrailers handles request trailers.
func (s *Server) HandleRequestTrailers(trailers *eppb.HttpTrailers) ([]*eppb.ProcessingResponse, error) {
	return []*eppb.ProcessingResponse{
		{
			Response: &eppb.ProcessingResponse_RequestTrailers{
				RequestTrailers: &eppb.TrailersResponse{},
			},
		},
	}, nil
}

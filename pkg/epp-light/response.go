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
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	envoy "sigs.k8s.io/gateway-api-inference-extension/pkg/common/envoy"
)

func (s *StreamingServer) handleResponseHeaders(reqCtx *requestContext, resp *extProcPb.ProcessingRequest_ResponseHeaders) {
	for _, header := range resp.ResponseHeaders.Headers.Headers {
		reqCtx.response.Headers[header.Key] = envoy.GetHeaderValue(header)
	}
}

func generateResponseHeaderResponse() *extProcPb.ProcessingResponse {
	return &extProcPb.ProcessingResponse{
		Response: &extProcPb.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extProcPb.HeadersResponse{
				Response: &extProcPb.CommonResponse{},
			},
		},
	}
}

func generateResponseBodyResponses(bodyBytes []byte, setEoS bool) []*extProcPb.ProcessingResponse {
	commonResponses := envoy.BuildChunkedBodyResponses(bodyBytes, setEoS)
	responses := make([]*extProcPb.ProcessingResponse, 0, len(commonResponses))
	for _, commonResp := range commonResponses {
		responses = append(responses, &extProcPb.ProcessingResponse{
			Response: &extProcPb.ProcessingResponse_ResponseBody{
				ResponseBody: &extProcPb.BodyResponse{
					Response: commonResp,
				},
			},
		})
	}
	return responses
}

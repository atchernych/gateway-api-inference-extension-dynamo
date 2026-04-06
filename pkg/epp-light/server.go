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
	"io"
	"math/rand"
	"net"
	"strings"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/log"

	envoy "sigs.k8s.io/gateway-api-inference-extension/pkg/common/envoy"
	errcommon "sigs.k8s.io/gateway-api-inference-extension/pkg/common/error"
	reqcommon "sigs.k8s.io/gateway-api-inference-extension/pkg/common/request"
)

// NewStreamingServer creates a new ext-proc streaming server with the given datastore and endpoint picker.
func NewStreamingServer(datastore Datastore, picker EndpointPicker) *StreamingServer {
	return &StreamingServer{
		datastore: datastore,
		picker:    picker,
	}
}

// StreamingServer implements the Envoy ext-proc server for the light EPP.
type StreamingServer struct {
	datastore Datastore
	picker    EndpointPicker
}

// requestContext stores state during the lifetime of an HTTP request.
type requestContext struct {
	targetEndpoint            string
	requestSize               int
	requestState              streamRequestState
	requestReceivedTimestamp  time.Time
	modelServerStreaming      bool

	request  *request
	response *response

	reqHeaderResp   *extProcPb.ProcessingResponse
	reqBodyResp     []*extProcPb.ProcessingResponse
	respHeaderResp  *extProcPb.ProcessingResponse
	respBodyResp    []*extProcPb.ProcessingResponse
	respTrailerResp *extProcPb.ProcessingResponse

	responseComplete bool
}

type request struct {
	Headers  map[string]string
	RawBody  []byte
	Metadata map[string]any
}

type response struct {
	Headers map[string]string
}

type streamRequestState int

const (
	stateRequestReceived                  streamRequestState = 0
	stateHeaderRequestResponseComplete    streamRequestState = 1
	stateBodyRequestResponsesComplete     streamRequestState = 2
	stateResponseReceived                 streamRequestState = 4
	stateHeaderResponseResponseComplete   streamRequestState = 5
	stateBodyResponseResponsesComplete    streamRequestState = 6
)

// Process implements the ext-proc bidirectional streaming RPC.
func (s *StreamingServer) Process(srv extProcPb.ExternalProcessor_ProcessServer) error {
	ctx := srv.Context()
	logger := log.FromContext(ctx)

	reqCtx := &requestContext{
		requestState: stateRequestReceived,
		request: &request{
			Headers:  make(map[string]string),
			Metadata: make(map[string]any),
		},
		response: &response{
			Headers: make(map[string]string),
		},
	}

	var body []byte
	var err error

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, recvErr := srv.Recv()
		if recvErr == io.EOF || status.Code(recvErr) == codes.Canceled {
			return nil
		}
		if recvErr != nil {
			return status.Errorf(codes.Unknown, "cannot receive stream request: %v", recvErr)
		}

		reqCtx.request.Metadata = envoy.ExtractMetadataValues(req)

		switch v := req.Request.(type) {
		case *extProcPb.ProcessingRequest_RequestHeaders:
			requestID := envoy.ExtractHeaderValue(v, reqcommon.RequestIdHeaderKey)
			if len(requestID) == 0 {
				requestID = uuid.NewString()
				reqCtx.request.Headers[reqcommon.RequestIdHeaderKey] = requestID
			}
			logger = logger.WithValues(reqcommon.RequestIdHeaderKey, requestID)
			logger.Info("Light EPP received request")
			ctx = log.IntoContext(ctx, logger)

			reqCtx.requestReceivedTimestamp = time.Now()
			s.handleRequestHeaders(reqCtx, v)

			// If EoS in headers, this is a header-only request (e.g. GET).
			// Route to a random endpoint.
			if v.RequestHeaders.EndOfStream {
				ep := s.getRandomEndpoint()
				if ep == nil {
					err = errcommon.Error{Code: errcommon.Internal, Msg: "no pods available in datastore"}
					break
				}
				reqCtx.targetEndpoint = net.JoinHostPort(ep.Address, ep.Port)
				reqCtx.reqHeaderResp = s.generateRequestHeaderResponse(ctx, reqCtx)
			}

		case *extProcPb.ProcessingRequest_RequestBody:
			body = append(body, v.RequestBody.Body...)
			if v.RequestBody.EndOfStream {
				reqCtx.request.RawBody = body
				reqCtx.requestSize = len(body)
				body = nil

				err = s.handleRequestBody(ctx, reqCtx)
			}

		case *extProcPb.ProcessingRequest_RequestTrailers:
			// Unused.

		case *extProcPb.ProcessingRequest_ResponseHeaders:
			for _, header := range v.ResponseHeaders.Headers.GetHeaders() {
				if header.Key == "content-type" && strings.Contains(string(header.RawValue), "text/event-stream") {
					reqCtx.modelServerStreaming = true
				}
			}
			reqCtx.requestState = stateResponseReceived
			s.handleResponseHeaders(reqCtx, v)
			reqCtx.respHeaderResp = generateResponseHeaderResponse()

		case *extProcPb.ProcessingRequest_ResponseBody:
			endOfStream := v.ResponseBody.EndOfStream
			chunk := v.ResponseBody.Body

			if endOfStream {
				reqCtx.responseComplete = true
			}

			if reqCtx.modelServerStreaming {
				reqCtx.respBodyResp = generateResponseBodyResponses(chunk, endOfStream)
			} else {
				body = append(body, chunk...)
				if endOfStream {
					reqCtx.responseComplete = true
					reqCtx.respBodyResp = generateResponseBodyResponses(body, true)
					body = nil
				}
			}

		case *extProcPb.ProcessingRequest_ResponseTrailers:
			if !reqCtx.responseComplete {
				reqCtx.responseComplete = true
				reqCtx.respBodyResp = generateResponseBodyResponses(body, false)
				body = nil
			}
			reqCtx.respTrailerResp = &extProcPb.ProcessingResponse{
				Response: &extProcPb.ProcessingResponse_ResponseTrailers{
					ResponseTrailers: &extProcPb.TrailersResponse{},
				},
			}
		}

		if err != nil {
			logger.Error(err, "Failed to process request")
			resp, buildErr := errcommon.BuildErrResponse(err)
			if buildErr != nil {
				return buildErr
			}
			if sendErr := srv.Send(resp); sendErr != nil {
				return status.Errorf(codes.Unknown, "failed to send error response: %v", sendErr)
			}
			return nil
		}

		if sendErr := reqCtx.updateStateAndSendIfNeeded(srv, logger); sendErr != nil {
			return sendErr
		}
	}
}

// handleRequestBody processes the full request body: extracts model, resolves
// candidate subset, filters endpoints, and calls the picker.
func (s *StreamingServer) handleRequestBody(ctx context.Context, reqCtx *requestContext) error {
	logger := log.FromContext(ctx)

	model := extractModelFromBody(reqCtx.request.RawBody)
	candidateSubset := extractCandidateSubset(reqCtx.request.Metadata)

	endpoints := s.datastore.ListEndpoints()
	if candidateSubset != nil {
		endpoints = filterEndpointsBySubset(endpoints, candidateSubset)
		if len(endpoints) == 0 {
			return errcommon.Error{
				Code: errcommon.ServiceUnavailable,
				Msg:  "no endpoints available matching candidate subset",
			}
		}
	}

	if len(endpoints) == 0 {
		return errcommon.Error{
			Code: errcommon.ServiceUnavailable,
			Msg:  "no endpoints available in pool",
		}
	}

	reqInfo := &RequestInfo{
		Headers:         reqCtx.request.Headers,
		Body:            reqCtx.request.RawBody,
		Model:           model,
		CandidateSubset: candidateSubset,
	}

	result, err := s.picker.Pick(ctx, reqInfo, endpoints)
	if err != nil {
		return errcommon.Error{
			Code: errcommon.ServiceUnavailable,
			Msg:  "endpoint picker failed: " + err.Error(),
		}
	}

	// Build the target endpoint string (primary + fallbacks).
	targetEndpoints := []string{result.Endpoint}
	targetEndpoints = append(targetEndpoints, result.Fallbacks...)
	reqCtx.targetEndpoint = strings.Join(targetEndpoints, ",")

	logger.Info("Request handled", "model", model, "endpoint", reqCtx.targetEndpoint)

	reqCtx.reqHeaderResp = s.generateRequestHeaderResponse(ctx, reqCtx)
	reqCtx.reqBodyResp = envoy.GenerateRequestBodyResponses(reqCtx.request.RawBody)

	return nil
}

func (s *StreamingServer) getRandomEndpoint() *Endpoint {
	endpoints := s.datastore.ListEndpoints()
	if len(endpoints) == 0 {
		return nil
	}
	ep := endpoints[rand.Intn(len(endpoints))]
	return &ep
}

// updateStateAndSendIfNeeded sends pending responses in the correct ext-proc order.
func (r *requestContext) updateStateAndSendIfNeeded(srv extProcPb.ExternalProcessor_ProcessServer, logger logr.Logger) error {
	if r.requestState == stateRequestReceived && r.reqHeaderResp != nil {
		if err := srv.Send(r.reqHeaderResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send request header response: %v", err)
		}
		r.requestState = stateHeaderRequestResponseComplete
	}
	if r.requestState == stateHeaderRequestResponseComplete && len(r.reqBodyResp) > 0 {
		for _, resp := range r.reqBodyResp {
			if err := srv.Send(resp); err != nil {
				return status.Errorf(codes.Unknown, "failed to send request body response: %v", err)
			}
		}
		logger.Info("Light EPP sent request body response(s) to proxy")
		r.requestState = stateBodyRequestResponsesComplete
		r.reqBodyResp = nil
	}
	if r.requestState == stateResponseReceived && r.respHeaderResp != nil {
		if err := srv.Send(r.respHeaderResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response header response: %v", err)
		}
		r.requestState = stateHeaderResponseResponseComplete
	}
	if r.requestState == stateHeaderResponseResponseComplete {
		for _, resp := range r.respBodyResp {
			if err := srv.Send(resp); err != nil {
				return status.Errorf(codes.Unknown, "failed to send response body response: %v", err)
			}
		}
		if r.responseComplete {
			logger.Info("Light EPP sent response body back to proxy")
			r.requestState = stateBodyResponseResponsesComplete
		}
		r.respBodyResp = nil
	}
	if r.requestState == stateBodyResponseResponsesComplete && r.respTrailerResp != nil {
		if err := srv.Send(r.respTrailerResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response trailer: %v", err)
		}
	}
	return nil
}

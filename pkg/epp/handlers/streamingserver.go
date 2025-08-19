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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"
	backendmetrics "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend/metrics"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/datastore"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metrics"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling"
	errutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/error"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

func NewStreamingServer(scheduler Scheduler, destinationEndpointHintMetadataNamespace, destinationEndpointHintKey string, datastore datastore.Datastore) *StreamingServer {
	return &StreamingServer{
		scheduler:                                scheduler,
		destinationEndpointHintMetadataNamespace: destinationEndpointHintMetadataNamespace,
		destinationEndpointHintKey:               destinationEndpointHintKey,
		datastore:                                datastore,
	}
}

type StreamingServer struct {
	scheduler Scheduler
	// The key of the header to specify the target pod address. This value needs to match Envoy
	// configuration.
	destinationEndpointHintKey string
	// The key acting as the outer namespace struct in the metadata extproc response to communicate
	// back the picked endpoints.
	destinationEndpointHintMetadataNamespace string
	datastore                                datastore.Datastore
}

func (s *StreamingServer) Process(srv extProcPb.ExternalProcessor_ProcessServer) error {
	ctx := srv.Context()
	logger := log.FromContext(ctx)
	loggerTrace := logger.V(logutil.TRACE)
	loggerTrace.Info("Processing")

	// Create request context to share states during life time of an HTTP request.
	// See https://github.com/envoyproxy/envoy/issues/17540.
	reqCtx := &RequestContext{
		RequestState: RequestReceived,
	}

	var body []byte
	var requestBody, responseBody map[string]interface{}

	// Create error handling var as each request should only report once for
	// error metrics. This doesn't cover the error "Cannot receive stream request" because
	// such errors might happen even though response is processed.
	var err error
	defer func(error, *RequestContext) {
		if reqCtx.ResponseStatusCode != "" {
			metrics.RecordRequestErrCounter(reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.ResponseStatusCode)
		} else if err != nil {
			metrics.RecordRequestErrCounter(reqCtx.Model, reqCtx.ResolvedTargetModel, errutil.CanonicalCode(err))
		}
		if reqCtx.RequestRunning {
			metrics.DecRunningRequests(reqCtx.Model)
		}
	}(err, reqCtx)

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
			// This error occurs very frequently, though it doesn't seem to have any impact.
			// TODO Figure out if we can remove this noise.
			logger.V(logutil.DEFAULT).Error(err, "Cannot receive stream request")
			return status.Errorf(codes.Unknown, "cannot receive stream request: %v", err)
		}

		switch v := req.Request.(type) {
		case *extProcPb.ProcessingRequest_RequestHeaders:
			err = s.HandleRequestHeaders(ctx, reqCtx, v)
		case *extProcPb.ProcessingRequest_RequestBody:
			loggerTrace.Info("Incoming body chunk", "EoS", v.RequestBody.EndOfStream)
			// In the stream case, we can receive multiple request bodies.
			body = append(body, v.RequestBody.Body...)

			// Message is buffered, we can read and decode.
			if v.RequestBody.EndOfStream {
				loggerTrace.Info("decoding")
				err = json.Unmarshal(body, &requestBody)
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Error unmarshaling request body")
				}

				// Body stream complete. Allocate empty slice for response to use.
				body = []byte{}

				reqCtx, err = s.HandleRequestBody(ctx, reqCtx, req, requestBody)
				if err != nil {
					logger.V(logutil.DEFAULT).Error(err, "Error handling body")
				} else {
					metrics.RecordRequestCounter(reqCtx.Model, reqCtx.ResolvedTargetModel)
					metrics.RecordRequestSizes(reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.RequestSize)
				}
			}
		case *extProcPb.ProcessingRequest_RequestTrailers:
			// This is currently unused.
		case *extProcPb.ProcessingRequest_ResponseHeaders:
			for _, header := range v.ResponseHeaders.Headers.GetHeaders() {
				value := string(header.RawValue)

				loggerTrace.Info("header", "key", header.Key, "value", value)
				if header.Key == "status" && value != "200" {
					reqCtx.ResponseStatusCode = errutil.ModelServerError
				} else if header.Key == "content-type" && strings.Contains(value, "text/event-stream") {
					reqCtx.modelServerStreaming = true
					loggerTrace.Info("model server is streaming response")
				}
			}
			reqCtx.RequestState = ResponseRecieved
			reqCtx.respHeaderResp = &extProcPb.ProcessingResponse{
				Response: &extProcPb.ProcessingResponse_ResponseHeaders{
					ResponseHeaders: &extProcPb.HeadersResponse{
						Response: &extProcPb.CommonResponse{
							HeaderMutation: &extProcPb.HeaderMutation{
								SetHeaders: []*configPb.HeaderValueOption{
									{
										Header: &configPb.HeaderValue{
											// This is for debugging purpose only.
											Key:      "x-went-into-resp-headers",
											RawValue: []byte("true"),
										},
									},
								},
							},
						},
					},
				},
			}

		case *extProcPb.ProcessingRequest_ResponseBody:
			if reqCtx.modelServerStreaming {
				// Currently we punt on response parsing if the modelServer is streaming, and we just passthrough.

				responseText := string(v.ResponseBody.Body)
				s.HandleResponseBodyModelStreaming(ctx, reqCtx, responseText)
				if v.ResponseBody.EndOfStream {
					loggerTrace.Info("stream completed")

					reqCtx.ResponseCompleteTimestamp = time.Now()
					metrics.RecordRequestLatencies(ctx, reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.RequestReceivedTimestamp, reqCtx.ResponseCompleteTimestamp)
					metrics.RecordResponseSizes(reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.ResponseSize)
				}

				reqCtx.respBodyResp = &extProcPb.ProcessingResponse{
					Response: &extProcPb.ProcessingResponse_ResponseBody{
						ResponseBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								BodyMutation: &extProcPb.BodyMutation{
									Mutation: &extProcPb.BodyMutation_StreamedResponse{
										StreamedResponse: &extProcPb.StreamedBodyResponse{
											Body:        v.ResponseBody.Body,
											EndOfStream: v.ResponseBody.EndOfStream,
										},
									},
								},
							},
						},
					},
				}
			} else {
				body = append(body, v.ResponseBody.Body...)

				// Message is buffered, we can read and decode.
				if v.ResponseBody.EndOfStream {
					loggerTrace.Info("stream completed")
					// Don't send a 500 on a response error. Just let the message passthrough and log our error for debugging purposes.
					// We assume the body is valid JSON, err messages are not guaranteed to be json, and so capturing and sending a 500 obfuscates the response message.
					// using the standard 'err' var will send an immediate error response back to the caller.
					var responseErr error
					responseErr = json.Unmarshal(body, &responseBody)
					if responseErr != nil {
						logger.V(logutil.DEFAULT).Error(responseErr, "Error unmarshaling request body")
					}

					reqCtx, responseErr = s.HandleResponseBody(ctx, reqCtx, responseBody)
					if responseErr != nil {
						logger.V(logutil.DEFAULT).Error(responseErr, "Failed to process response body", "request", req)
					} else if reqCtx.ResponseComplete {
						reqCtx.ResponseCompleteTimestamp = time.Now()
						metrics.RecordRequestLatencies(ctx, reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.RequestReceivedTimestamp, reqCtx.ResponseCompleteTimestamp)
						metrics.RecordResponseSizes(reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.ResponseSize)
						metrics.RecordInputTokens(reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.Usage.PromptTokens)
						metrics.RecordOutputTokens(reqCtx.Model, reqCtx.ResolvedTargetModel, reqCtx.Usage.CompletionTokens)
					}
				}
			}
		case *extProcPb.ProcessingRequest_ResponseTrailers:
			// This is currently unused.
		}

		// Handle the err and fire an immediate response.
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to process request", "request", req)
			resp, err := BuildErrResponse(err)
			if err != nil {
				return err
			}
			if err := srv.Send(resp); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Send failed")
				return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
			}
			return nil
		}
		loggerTrace.Info("checking", "request state", reqCtx.RequestState)
		if err := reqCtx.updateStateAndSendIfNeeded(srv, logger); err != nil {
			return err
		}
	}
}

// updateStateAndSendIfNeeded checks state and can send mutiple responses in a single pass, but only if ordered properly.
// Order of requests matter in FULL_DUPLEX_STREAMING. For both request and response, the order of response sent back MUST be: Header->Body->Trailer, with trailer being optional.
func (r *RequestContext) updateStateAndSendIfNeeded(srv extProcPb.ExternalProcessor_ProcessServer, logger logr.Logger) error {
	loggerTrace := logger.V(logutil.TRACE)
	// No switch statement as we could send multiple responses in one pass.
	if r.RequestState == RequestReceived && r.reqHeaderResp != nil {
		loggerTrace.Info("Sending request header response", "obj", r.reqHeaderResp)
		if err := srv.Send(r.reqHeaderResp); err != nil {
			logger.V(logutil.DEFAULT).Error(err, "error sending response")
			return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
		}
		r.RequestState = HeaderRequestResponseComplete
	}
	if r.RequestState == HeaderRequestResponseComplete && r.reqBodyResp != nil {
		loggerTrace.Info("Sending request body response")
		if err := srv.Send(r.reqBodyResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
		}
		r.RequestState = BodyRequestResponsesComplete
		metrics.IncRunningRequests(r.Model)
		r.RequestRunning = true
		// Dump the response so a new stream message can begin
		r.reqBodyResp = nil
	}
	if r.RequestState == BodyRequestResponsesComplete && r.reqTrailerResp != nil {
		// Trailers in requests are not guaranteed
		if err := srv.Send(r.reqHeaderResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
		}
	}
	if r.RequestState == ResponseRecieved && r.respHeaderResp != nil {
		loggerTrace.Info("Sending response header response", "obj", r.respHeaderResp)
		if err := srv.Send(r.respHeaderResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
		}
		r.RequestState = HeaderResponseResponseComplete
	}
	if r.RequestState == HeaderResponseResponseComplete && r.respBodyResp != nil {
		loggerTrace.Info("Sending response body response")
		if err := srv.Send(r.respBodyResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
		}

		body := r.respBodyResp.Response.(*extProcPb.ProcessingResponse_ResponseBody)
		if body.ResponseBody.Response.GetBodyMutation().GetStreamedResponse().GetEndOfStream() {
			r.RequestState = BodyResponseResponsesComplete
		}
		// Dump the response so a new stream message can begin
		r.respBodyResp = nil
	}
	if r.RequestState == BodyResponseResponsesComplete && r.respTrailerResp != nil {
		// Trailers in requests are not guaranteed
		if err := srv.Send(r.reqHeaderResp); err != nil {
			return status.Errorf(codes.Unknown, "failed to send response back to Envoy: %v", err)
		}
	}
	return nil
}

// HandleRequestBody always returns the requestContext even in the error case, as the request context is used in error handling.
func (s *StreamingServer) HandleRequestBody(
	ctx context.Context,
	reqCtx *RequestContext,
	req *extProcPb.ProcessingRequest,
	requestBodyMap map[string]interface{},
) (*RequestContext, error) {
	var requestBodyBytes []byte
	logger := log.FromContext(ctx)

	// Resolve target models.
	model, ok := requestBodyMap["model"].(string)
	if !ok {
		return reqCtx, errutil.Error{Code: errutil.BadRequest, Msg: "model not found in request"}
	}

	modelName := model

	// NOTE: The nil checking for the modelObject means that we DO allow passthrough currently.
	// This might be a security risk in the future where adapters not registered in the InferenceModel
	// are able to be requested by using their distinct name.
	modelObj := s.datastore.ModelGet(model)
	if modelObj == nil {
		return reqCtx, errutil.Error{Code: errutil.BadConfiguration, Msg: fmt.Sprintf("error finding a model object in InferenceModel for input %v", model)}
	}
	if len(modelObj.Spec.TargetModels) > 0 {
		modelName = RandomWeightedDraw(logger, modelObj, 0)
		if modelName == "" {
			return reqCtx, errutil.Error{Code: errutil.BadConfiguration, Msg: fmt.Sprintf("error getting target model name for model %v", modelObj.Name)}
		}
	}
	llmReq := &scheduling.LLMRequest{
		Model:               model,
		ResolvedTargetModel: modelName,
		Critical:            datastore.IsCritical(modelObj),
	}
	logger.V(logutil.DEBUG).Info("LLM request assembled", "model", llmReq.Model, "targetModel", llmReq.ResolvedTargetModel, "critical", llmReq.Critical)

	var err error
	// Update target models in the body.
	if llmReq.Model != llmReq.ResolvedTargetModel {
		requestBodyMap["model"] = llmReq.ResolvedTargetModel
	}

	requestBodyBytes, err = json.Marshal(requestBodyMap)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Error marshaling request body")
		return reqCtx, errutil.Error{Code: errutil.Internal, Msg: fmt.Sprintf("error marshaling request body: %v", err)}
	}

	// Ask the Dynamo FrontEnd for worker selection.
	if workerID, feErr := s.callFrontEndForWorker(ctx, requestBodyMap); feErr != nil {
		// Proceed without a worker_instance_id if FrontEnd is unavailable.
		logger.V(logutil.DEFAULT).Error(feErr, "FrontEnd call failed. Continuing without worker_instance_id")
	} else if workerID != "" {
		reqCtx.WorkerInstanceID = workerID
		logger.V(logutil.VERBOSE).Info("Extracted worker instance ID from FrontEnd", "worker_instance_id", workerID)
	}

	target, err := s.scheduler.Schedule(ctx, llmReq)
	if err != nil {
		return reqCtx, errutil.Error{Code: errutil.InferencePoolResourceExhausted, Msg: fmt.Errorf("failed to find target pod: %w", err).Error()}
	}
	targetPod := target.GetPod()

	// Insert target endpoint to instruct Envoy to route requests to the specified target pod.
	// Attach the port number
	pool, err := s.datastore.PoolGet()
	if err != nil {
		return reqCtx, err
	}
	endpoint := targetPod.Address + ":" + strconv.Itoa(int(pool.Spec.TargetPortNumber))

	logger.V(logutil.DEFAULT).Info("Request handled",
		"model", llmReq.Model, "targetModel", llmReq.ResolvedTargetModel, "endpoint", targetPod, "endpoint metrics",
		fmt.Sprintf("%+v", target))

	reqCtx.Model = llmReq.Model
	reqCtx.ResolvedTargetModel = llmReq.ResolvedTargetModel
	reqCtx.RequestSize = len(requestBodyBytes)
	reqCtx.TargetPod = targetPod.NamespacedName.String()
	reqCtx.TargetEndpoint = endpoint

	s.populateRequestHeaderResponse(reqCtx, endpoint, len(requestBodyBytes))

	reqCtx.reqBodyResp = &extProcPb.ProcessingResponse{
		// The Endpoint Picker supports two approaches to communicating the target endpoint, as a request header
		// and as an unstructure ext-proc response metadata key/value pair. This enables different integration
		// options for gateway providers.
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
	}
	return reqCtx, nil
}

// HandleResponseBody always returns the requestContext even in the error case, as the request context is used in error handling.
func (s *StreamingServer) HandleResponseBody(
	ctx context.Context,
	reqCtx *RequestContext,
	response map[string]interface{},
) (*RequestContext, error) {
	logger := log.FromContext(ctx)
	responseBytes, err := json.Marshal(response)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "error marshalling responseBody")
		return reqCtx, err
	}
	if response["usage"] != nil {
		usg := response["usage"].(map[string]interface{})
		usage := Usage{
			PromptTokens:     int(usg["prompt_tokens"].(float64)),
			CompletionTokens: int(usg["completion_tokens"].(float64)),
			TotalTokens:      int(usg["total_tokens"].(float64)),
		}
		reqCtx.Usage = usage
		logger.V(logutil.VERBOSE).Info("Response generated", "usage", reqCtx.Usage)
	}
	reqCtx.ResponseSize = len(responseBytes)
	// ResponseComplete is to indicate the response is complete. In non-streaming
	// case, it will be set to be true once the response is processed; in
	// streaming case, it will be set to be true once the last chunk is processed.
	// TODO(https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/178)
	// will add the processing for streaming case.
	reqCtx.ResponseComplete = true

	reqCtx.respBodyResp = &extProcPb.ProcessingResponse{
		// The Endpoint Picker supports two approaches to communicating the target endpoint, as a request header
		// and as an unstructure ext-proc response metadata key/value pair. This enables different integration
		// options for gateway providers.
		Response: &extProcPb.ProcessingResponse_ResponseBody{
			ResponseBody: &extProcPb.BodyResponse{
				Response: &extProcPb.CommonResponse{
					BodyMutation: &extProcPb.BodyMutation{
						Mutation: &extProcPb.BodyMutation_StreamedResponse{
							StreamedResponse: &extProcPb.StreamedBodyResponse{
								Body:        responseBytes,
								EndOfStream: true,
							},
						},
					},
				},
			},
		},
	}
	return reqCtx, nil
}

// The function is to handle streaming response if the modelServer is streaming.
func (s *StreamingServer) HandleResponseBodyModelStreaming(
	ctx context.Context,
	reqCtx *RequestContext,
	responseText string,
) {
	if strings.Contains(responseText, streamingEndMsg) {
		resp := ParseRespForUsage(ctx, responseText)
		metrics.RecordInputTokens(reqCtx.Model, reqCtx.ResolvedTargetModel, resp.Usage.PromptTokens)
		metrics.RecordOutputTokens(reqCtx.Model, reqCtx.ResolvedTargetModel, resp.Usage.CompletionTokens)
	}
}

func (s *StreamingServer) HandleRequestHeaders(ctx context.Context, reqCtx *RequestContext, req *extProcPb.ProcessingRequest_RequestHeaders) error {
	reqCtx.RequestReceivedTimestamp = time.Now()

	// an EoS in the request headers means this request has no body or trailers.
	if req.RequestHeaders.EndOfStream {
		// We will route this request to a random pod as this is assumed to just be a GET
		// More context: https://github.com/kubernetes-sigs/gateway-api-inference-extension/pull/526
		// The above PR will address endpoint admission, but currently any request without a body will be
		// routed to a random upstream pod.
		pod := GetRandomPod(s.datastore)
		pool, err := s.datastore.PoolGet()
		if err != nil {
			return err
		}
		endpoint := pod.Address + ":" + strconv.Itoa(int(pool.Spec.TargetPortNumber))
		s.populateRequestHeaderResponse(reqCtx, endpoint, 0)
	}
	return nil
}

func (s *StreamingServer) populateRequestHeaderResponse(reqCtx *RequestContext, endpoint string, requestBodyLength int) {
	headers := []*configPb.HeaderValueOption{
		{
			Header: &configPb.HeaderValue{
				Key:      s.destinationEndpointHintKey,
				RawValue: []byte(endpoint),
			},
		},
	}
	if requestBodyLength > 0 {
		// We need to update the content length header if the body is mutated, see Envoy doc:
		// https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/http/ext_proc/v3/processing_mode.proto
		headers = append(headers, &configPb.HeaderValueOption{
			Header: &configPb.HeaderValue{
				Key:      "Content-Length",
				RawValue: []byte(strconv.Itoa(requestBodyLength)),
			},
		})
	}
	// Inject worker_instance_id reported by Dynamo router so the gateway can route to the chosen worker.
	logger := log.Log.WithName("handlers").WithName("populateRequestHeaderResponse")
	if reqCtx.WorkerInstanceID != "" {
		headers = append(headers, &configPb.HeaderValueOption{
			Header: &configPb.HeaderValue{
				Key:      "x-worker-instance-id",
				RawValue: []byte(reqCtx.WorkerInstanceID),
			},
		})
		logger.V(logutil.VERBOSE).Info(
			"Injected x-worker-instance-id header",
			"worker_instance_id", reqCtx.WorkerInstanceID,
		)
	} else {
		logger.V(logutil.VERBOSE).Info(
			"Did not inject x-worker-instance-id header (empty worker id)",
		)
	}

	targetEndpointValue := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			s.destinationEndpointHintKey: {
				Kind: &structpb.Value_StringValue{
					StringValue: endpoint,
				},
			},
		},
	}
	dynamicMetadata := targetEndpointValue
	if s.destinationEndpointHintMetadataNamespace != "" {
		// If a namespace is defined, wrap the selected endpoint with that.
		dynamicMetadata = &structpb.Struct{
			Fields: map[string]*structpb.Value{
				s.destinationEndpointHintMetadataNamespace: {
					Kind: &structpb.Value_StructValue{
						StructValue: targetEndpointValue,
					},
				},
			},
		}
	}

	reqCtx.reqHeaderResp = &extProcPb.ProcessingResponse{
		Response: &extProcPb.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extProcPb.HeadersResponse{
				Response: &extProcPb.CommonResponse{
					ClearRouteCache: true,
					HeaderMutation: &extProcPb.HeaderMutation{
						SetHeaders: headers,
					},
				},
			},
		},
		DynamicMetadata: dynamicMetadata,
	}
}

// Blocking call to the FrontEnd to obtain the Dynamo worker_instance_id for routing.
func (s *StreamingServer) callFrontEndForWorker(ctx context.Context, originalBody map[string]interface{}) (string, error) {
	logger := log.FromContext(ctx)
	feURL := "http://127.0.0.1:8000/v1/chat/completions"

	feBody := make(map[string]interface{}, len(originalBody)+1)
	for k, v := range originalBody {
		feBody[k] = v
	}
	// Make sure we send the streaming type request.
	if _, ok := feBody["stream"]; !ok {
		feBody["stream"] = true
	}
	nvext, _ := feBody["nvext"].(map[string]interface{})
	if nvext == nil {
		nvext = map[string]interface{}{}
	}
	var anns []string
	switch v := nvext["annotations"].(type) {
	case []string:
		anns = append(anns, v...)
	case []interface{}:
		for _, x := range v {
			if str, ok := x.(string); ok {
				anns = append(anns, str)
			}
		}
	}
	needQueryInstanceID := "query_instance_id"
	found := false
	for _, a := range anns {
		if a == needQueryInstanceID {
			found = true
			break
		}
	}
	if !found {
		anns = append(anns, needQueryInstanceID)
	}
	nvext["annotations"] = anns
	feBody["nvext"] = nvext

	payload, err := json.Marshal(feBody)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd marshal failed")
		return "", fmt.Errorf("marshal FrontEnd body: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, feURL, bytes.NewReader(payload))
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, " Dynamo FrontEnd request build failed")
		return "", fmt.Errorf("build FrontEnd request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0}

	resp, err := client.Do(req)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, " Dynamo FrontEnd POST failed")
		return "", fmt.Errorf("FrontEnd POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(resp.Body)
		logger.V(logutil.DEFAULT).Error(nil, "Dynamo FrontEnd non-2xx response",
			"status_code", resp.StatusCode,
			"response_body", string(errBody),
		)
		return "", fmt.Errorf("Dynamo FrontEnd error: %d body=%s", resp.StatusCode, string(errBody))
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(ct, "text/event-stream") {
		logger.V(logutil.DEFAULT).Error(nil, "Unexpected non-SSE response")
		return "", fmt.Errorf("unexpected non-SSE response (Content-Type=%q)", resp.Header.Get("Content-Type"))
	}

	// We expect the information in the following format:
	// event: worker_instance_id
	// : "8303679623149182543"
	//
	// data: [DONE]
	reader := bufio.NewReader(resp.Body)

	var (
		eventName  string
		dataBuf    strings.Builder // accumulates one event's data: lines
		commentBuf strings.Builder // accumulates one event's comment (":") lines
	)

	flushEvent := func() (string, bool, error) {
		data := strings.TrimSpace(dataBuf.String())
		comment := strings.TrimSpace(commentBuf.String())
		dataBuf.Reset()
		commentBuf.Reset()

		// Handle [DONE]
		if data == "[DONE]" || comment == "[DONE]" {
			logger.V(logutil.DEFAULT).Info("SSE stream DONE")
			return "", true, nil
		}

		// If this is the special event carrying the id by name, prefer its payload
		if eventName == "worker_instance_id" {
			// The Dynamo FrontEnd puts the id on a comment line, quoted:  : "8303..."
			candidate := data
			if candidate == "" {
				candidate = comment
			}
			if candidate != "" {
				// Try JSON string first (e.g. "8303679...")
				var s string
				if json.Unmarshal([]byte(candidate), &s) == nil && s != "" {
					logger.V(logutil.VERBOSE).Info("Dynamo worker_instance_id extracted from named event",
						"worker_instance_id", s)
					return s, false, nil
				}
				// Fallback: raw strip quotes
				clean := strings.Trim(candidate, "\"")
				if clean != "" && clean != "[DONE]" {
					logger.V(logutil.DEFAULT).Info("Dynamo worker_instance_id extracted (raw) from named event",
						"worker_instance_id", clean)
					return clean, false, nil
				}
			}
		}

		// Generic JSON object path (if someone sends data: {...})
		if data != "" {
			var msg map[string]interface{}
			if json.Unmarshal([]byte(data), &msg) == nil {
				// top-level
				if wid, ok := msg["worker_instance_id"].(string); ok && wid != "" {
					logger.V(logutil.DEFAULT).Info("Dynamo worker_instance_id found in SSE payload root",
						"worker_instance_id", wid)
					return wid, false, nil
				}
				// annotations map
				if ann, ok := msg["annotations"].(map[string]interface{}); ok {
					if wid, ok := ann["worker_instance_id"].(string); ok && wid != "" {
						logger.V(logutil.DEFAULT).Info("Dynamo worker_instance_id found in SSE annotations",
							"worker_instance_id", wid)
						return wid, false, nil
					}
				}
			}
		}
		return "", false, nil
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// Process any pending event on EOF
				if wid, done, _ := flushEvent(); wid != "" {
					// All went well. Returning the worker_instance_id
					return wid, nil
				} else if done {
					return "", fmt.Errorf("Dynamo worker_instance_id not found before DONE")
				}
				logger.V(logutil.DEFAULT).Error(nil, "EOF before worker_instance_id")
				return "", fmt.Errorf("worker_instance_id not found in SSE stream (EOF)")
			}
			logger.V(logutil.DEFAULT).Error(err, "SSE read error")
			return "", fmt.Errorf("sse read error: %w", err)
		}

		l := strings.TrimRight(line, "\r\n")
		if l == "" {
			// End of event: process accumulated buffers
			if wid, done, _ := flushEvent(); wid != "" {
				return wid, nil
			} else if done {
				return "", fmt.Errorf("Dynamo worker_instance_id not found before DONE")
			}
			eventName = "" // reset for next event
			continue
		}

		// Comment line
		if strings.HasPrefix(l, ":") {
			commentLine := strings.TrimSpace(l[1:])
			if commentBuf.Len() > 0 {
				commentBuf.WriteByte('\n')
			}
			commentBuf.WriteString(commentLine)
			continue
		}

		// field: value
		if idx := strings.IndexByte(l, ':'); idx != -1 {
			field := l[:idx]
			val := strings.TrimSpace(l[idx+1:])
			switch field {
			case "event":
				eventName = val
			case "data":
				if dataBuf.Len() > 0 {
					dataBuf.WriteByte('\n')
				}
				dataBuf.WriteString(val)
			default:
				// ignore other fields (id, retry, etc.)
			}
		}
	}
}

func RandomWeightedDraw(logger logr.Logger, model *v1alpha2.InferenceModel, seed int64) string {
	// TODO: after we are down to 1 server implementation, make these methods a part of the struct
	// and handle random seeding on the struct.
	source := rand.NewSource(rand.Int63())
	if seed > 0 {
		source = rand.NewSource(seed)
	}
	r := rand.New(source)

	// all the weight values are nil, then we should return random model name
	if model.Spec.TargetModels[0].Weight == nil {
		index := r.Int31n(int32(len(model.Spec.TargetModels)))
		return model.Spec.TargetModels[index].Name
	}

	var weights int32
	for _, model := range model.Spec.TargetModels {
		weights += *model.Weight
	}
	logger.V(logutil.TRACE).Info("Weights for model computed", "model", model.Name, "weights", weights)
	randomVal := r.Int31n(weights)
	// TODO: optimize this without using loop
	for _, model := range model.Spec.TargetModels {
		if randomVal < *model.Weight {
			return model.Name
		}
		randomVal -= *model.Weight
	}
	return ""
}

func GetRandomPod(ds datastore.Datastore) *backendmetrics.Pod {
	pods := ds.PodGetAll()
	number := rand.Intn(len(pods))
	pod := pods[number]
	return pod.GetPod()
}

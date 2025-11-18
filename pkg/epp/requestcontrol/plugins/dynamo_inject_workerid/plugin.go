package dynamo_inject_workerid

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	rc "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/requestcontrol"
	schedtypes "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
)

const (
	typeString      = "dynamo-inject-workerid"
	pluginName      = "dynamo-inject-workerid"
	WorkerIDHeader  = "x-worker-instance-id"
	TokenDataHeader = "x-epp-inject-nvext-token-data"
)

var _ plugins.Plugin = (*InjectWorkerIDPreRequest)(nil)
var _ rc.PreRequest = (*InjectWorkerIDPreRequest)(nil)
var _ rc.RequestBodyMutator = (*InjectWorkerIDPreRequest)(nil)

type InjectWorkerIDPreRequest struct {
	typedName plugins.TypedName
}

func NewInjectWorkerIDPreRequest() *InjectWorkerIDPreRequest {
	return &InjectWorkerIDPreRequest{
		typedName: plugins.TypedName{Type: typeString, Name: pluginName},
	}
}

func (p *InjectWorkerIDPreRequest) WithName(name string) *InjectWorkerIDPreRequest {
	p.typedName.Name = name
	return p
}

func InjectWorkerIDPreRequestFactory(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	return NewInjectWorkerIDPreRequest().WithName(name), nil
}

func (p *InjectWorkerIDPreRequest) TypedName() plugins.TypedName { return p.typedName }

func (p *InjectWorkerIDPreRequest) PreRequest(
	_ context.Context,
	req *schedtypes.LLMRequest,
	_ *schedtypes.SchedulingResult,
	_ int,
) {
	if req == nil {
		return
	}
	if req.Headers == nil {
		req.Headers = map[string]string{}
	}
	wid := strings.TrimSpace(req.Headers[WorkerIDHeader])
	if wid == "" {
		return
	}
	req.Headers[WorkerIDHeader] = wid
}

func (p *InjectWorkerIDPreRequest) MutateRequestBody(
	_ context.Context,
	req *schedtypes.LLMRequest,
	_ *schedtypes.SchedulingResult,
	_ int,
	body map[string]any,
) {
	if req == nil || body == nil {
		return
	}
	if req.Headers == nil {
		return
	}

	wid := strings.TrimSpace(req.Headers[WorkerIDHeader])
	if wid == "" {
		return
	}

	nvext, _ := body["nvext"].(map[string]any)
	if nvext == nil {
		nvext = map[string]any{}
		body["nvext"] = nvext
	}
	nvext["backend_instance_id"] = wid

	if td := strings.TrimSpace(req.Headers[TokenDataHeader]); td != "" {
		if raw, err := base64.StdEncoding.DecodeString(td); err == nil {
			var tokens []int64
			if err := json.Unmarshal(raw, &tokens); err == nil && len(tokens) > 0 {
				nvext["token_data"] = tokens
			}
		}
		delete(req.Headers, TokenDataHeader)
	}
}

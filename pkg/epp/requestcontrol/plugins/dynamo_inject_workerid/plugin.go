package dynamo_inject_workerid

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	rc "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/requestcontrol"
	schedtypes "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
)

const (
	typeString             = "dynamo-inject-workerid"
	pluginName             = "dynamo-inject-workerid"
	WorkerIDHeader         = "x-worker-instance-id"
	PrefillWorkerIDHeader  = "x-prefiller-host-port"
	tokenDataAnnotationKey = "dynamo/token-data"
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

	// Handle worker instance ID
	wid := strings.TrimSpace(req.Headers[WorkerIDHeader])
	if wid != "" {
		req.Headers[WorkerIDHeader] = wid
	}

	// Handle prefill worker ID
	prefillWid := strings.TrimSpace(req.Headers[PrefillWorkerIDHeader])
	if prefillWid != "" {
		req.Headers[PrefillWorkerIDHeader] = prefillWid
	}
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

	prefillWid := strings.TrimSpace(req.Headers[PrefillWorkerIDHeader])

	nvext, _ := body["nvext"].(map[string]any)
	if nvext == nil {
		nvext = map[string]any{}
		body["nvext"] = nvext
	}

	if prefillWid != "" {
		// Disaggregated mode: use prefill_worker_id and decode_worker_id
		if prefillWidUint, err := strconv.ParseUint(prefillWid, 10, 64); err == nil {
			nvext["prefill_worker_id"] = prefillWidUint
		}
		if widUint, err := strconv.ParseUint(wid, 10, 64); err == nil {
			nvext["decode_worker_id"] = widUint
		}
	} else {
		// Non-disaggregated mode: use backend_instance_id
		if widUint, err := strconv.ParseUint(wid, 10, 64); err == nil {
			nvext["backend_instance_id"] = widUint
		}
	}

	if tokens, ok := req.Annotations[tokenDataAnnotationKey]; ok {
		switch v := tokens.(type) {
		case []int64:
			if len(v) > 0 {
				nvext["token_data"] = v
			}
		case []any:
			var out []int64
			for _, elem := range v {
				switch t := elem.(type) {
				case int64:
					out = append(out, t)
				case float64:
					out = append(out, int64(t))
				}
			}
			if len(out) > 0 {
				nvext["token_data"] = out
			}
		case json.RawMessage:
			var out []int64
			if err := json.Unmarshal(v, &out); err == nil && len(out) > 0 {
				nvext["token_data"] = out
			}
		}
	}

	// Remove query_instance_id from nvext.annotations if present
	if annotations, ok := nvext["annotations"]; ok {
		switch annList := annotations.(type) {
		case []string:
			filtered := make([]string, 0, len(annList))
			for _, ann := range annList {
				if ann != "query_instance_id" {
					filtered = append(filtered, ann)
				}
			}
			if len(filtered) == 0 {
				delete(nvext, "annotations")
			} else {
				nvext["annotations"] = filtered
			}
		case []any:
			filtered := make([]any, 0, len(annList))
			for _, ann := range annList {
				if str, ok := ann.(string); !ok || str != "query_instance_id" {
					filtered = append(filtered, ann)
				}
			}
			if len(filtered) == 0 {
				delete(nvext, "annotations")
			} else {
				nvext["annotations"] = filtered
			}
		}
	}
}

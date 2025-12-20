package dynamo_cleanup

import (
	"context"
	"encoding/json"

	log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	rc "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/requestcontrol"
	schedtypes "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"

	dynamo "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/plugins/dynamo_kv_scorer"
)

const (
	PluginName = "dynamo-cleanup"
	PluginType = "dynamo-cleanup"
)

// DynamoCleanupPlugin is a PostResponse plugin that cleans up router state
// when a request completes. It calls dynamo_router_free_request to release
// the bookkeeping resources associated with the request.
type DynamoCleanupPlugin struct {
	typedName plugins.TypedName
}

var _ plugins.Plugin = (*DynamoCleanupPlugin)(nil)
var _ rc.PostResponse = (*DynamoCleanupPlugin)(nil)

// NewDynamoCleanupPlugin creates a new DynamoCleanupPlugin instance.
func NewDynamoCleanupPlugin() *DynamoCleanupPlugin {
	return &DynamoCleanupPlugin{
		typedName: plugins.TypedName{Type: PluginType, Name: PluginName},
	}
}

// WithName sets a custom name for the plugin.
func (p *DynamoCleanupPlugin) WithName(name string) *DynamoCleanupPlugin {
	p.typedName.Name = name
	return p
}

// DynamoCleanupPluginFactory creates a DynamoCleanupPlugin from configuration.
func DynamoCleanupPluginFactory(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	return NewDynamoCleanupPlugin().WithName(name), nil
}

// TypedName returns the plugin's type and name.
func (p *DynamoCleanupPlugin) TypedName() plugins.TypedName {
	return p.typedName
}

// PostResponse is called after a response is received from the model server.
// It cleans up the router bookkeeping state for the completed request.
func (p *DynamoCleanupPlugin) PostResponse(
	ctx context.Context,
	request *schedtypes.LLMRequest,
	response *rc.Response,
	targetPod *backend.Pod,
) {
	logger := log.FromContext(ctx)

	if request == nil {
		logger.V(logutil.DEBUG).Info("DynamoCleanupPlugin: request is nil, skipping cleanup")
		return
	}

	requestID := request.RequestId
	if requestID == "" {
		logger.V(logutil.DEBUG).Info("DynamoCleanupPlugin: no request ID, skipping cleanup")
		return
	}

	// Call the dynamo router to free the request bookkeeping
	if err := dynamo.CallFreeRequest(requestID); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "DynamoCleanupPlugin: failed to free request",
			"requestID", requestID)
		return
	}

	logger.V(logutil.VERBOSE).Info("DynamoCleanupPlugin: freed request from router",
		"requestID", requestID)
}


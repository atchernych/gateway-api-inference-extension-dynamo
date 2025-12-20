package requestcontrol

import (
	"context"

	schedtypes "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
)

// RequestBodyMutator allows pre-request plugins to mutate the outbound request body.
// Implementations are invoked after the standard PreRequest hook completes.
type RequestBodyMutator interface {
	MutateRequestBody(
		ctx context.Context,
		request *schedtypes.LLMRequest,
		schedulingResult *schedtypes.SchedulingResult,
		targetPort int,
		body map[string]any,
	)
}

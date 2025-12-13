package dynamo_kv_scorer

/*
#cgo CPPFLAGS: -I${SRCDIR}/include
#cgo CXXFLAGS: -std=c++17
#cgo LDFLAGS: ${SRCDIR}/lib/libdynamo_llm_capi.a -lstdc++ -ldl -lpthread -lm

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>   // for free
#include <stdbool.h>

// enum underlying type is uint32_t; matches cbindgen output
typedef uint32_t dynamo_llm_result_t;
enum { DYNAMO_OK = 0, DYNAMO_ERR = 1 };

// opaque handle forward-decl
struct WorkerSelectionPipeline;
typedef struct WorkerSelectionPipeline WorkerSelectionPipeline;

// Prototypes (C-compatible)
dynamo_llm_result_t dynamo_llm_init(const char *namespace_c_str,
                                    const char *component_c_str,
                                    int64_t worker_id,
                                    uint32_t kv_block_size);

dynamo_llm_result_t dynamo_llm_shutdown(void);
dynamo_llm_result_t dynamo_llm_load_publisher_create(void);

dynamo_llm_result_t dynamo_kv_event_publish_stored(uint64_t event_id,
                                                   const uint32_t *token_ids,
                                                   const uintptr_t *num_block_tokens,
                                                   const uint64_t *block_ids,
                                                   size_t num_blocks,
                                                   const uint64_t *parent_hash,
                                                   uint64_t lora_id);

dynamo_llm_result_t dynamo_kv_event_publish_removed(uint64_t event_id,
                                                    const uint64_t *block_ids,
                                                    size_t num_blocks);

dynamo_llm_result_t dynamo_create_worker_selection_pipeline(const char *namespace_c_str,
                                                            const char *component_c_str,
                                                            const char *model_name_c_str,
                                                            bool use_kv_routing,
                                                            double busy_threshold,
                                                            double overlap_score_weight,
                                                            double router_temperature,
                                                            bool use_kv_events,
                                                            bool router_replica_sync,
                                                            WorkerSelectionPipeline **pipeline_out);

dynamo_llm_result_t dynamo_destroy_worker_selection_pipeline(WorkerSelectionPipeline *pipeline);

dynamo_llm_result_t dynamo_query_worker_selection_and_annotate(WorkerSelectionPipeline *pipeline,
                                                               const char *request_json_c_str,
                                                               int64_t *worker_instance_id_out,
                                                               int64_t *prefill_worker_id_out,
                                                               uint32_t **token_ids_out,
                                                               size_t *token_count_out,
                                                               char **annotated_request_json_out);

dynamo_llm_result_t dynamo_free_worker_selection_result(uint32_t *token_ids,
                                                        size_t token_count,
                                                        char *annotated_request_json);
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"unsafe"

	log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/framework"
	schedtypes "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	PluginName               = "dynamo-kv-scorer"
	KVAwareScorerType        = "kv-aware-scorer"
	StateKeyWorkerInstanceID = schedtypes.StateKey("dynamo/worker-instance-id")
	StateKeyPrefillWorkerID  = schedtypes.StateKey("dynamo/prefill-worker-id")
	WorkerIDHeader           = "x-worker-instance-id"
	PrefillWorkerIDHeader    = "x-prefiller-host-port"
	tokenDataAnnotationKey   = "dynamo/token-data"
)

// --------------------------- config / env ---------------------------

var warmupOnce sync.Once
var warmupErr error

type stateString string
type params struct {
}

func (s stateString) Clone() schedtypes.StateData { return s }

type KVAwareScorer struct {
	typedName plugins.TypedName
}

var _ plugins.Plugin = (*KVAwareScorer)(nil)
var _ framework.Scorer = (*KVAwareScorer)(nil)

func NewKVAwareScorer() *KVAwareScorer {
	return &KVAwareScorer{
		typedName: plugins.TypedName{Type: KVAwareScorerType, Name: PluginName},
	}
}

func (k *KVAwareScorer) WithName(name string) *KVAwareScorer { k.typedName.Name = name; return k }

func KVAwareScorerFactory(name string, raw json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	p := params{}
	_ = json.Unmarshal(raw, &p)

	s := NewKVAwareScorer().WithName(name)

	// one-time FFI init (runtime + persistent pipeline)
	warmupOnce.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				warmupErr = fmt.Errorf("Dynamo configuration error: %v", r)
			}
		}()
		warmupErr = initFFI()
	})
	if warmupErr != nil {
		return nil, fmt.Errorf("Dynamo FFI init for the Router failed: %w", warmupErr)
	}

	return s, nil
}

func (k *KVAwareScorer) TypedName() plugins.TypedName { return k.typedName }

// --------------------------- FFI integration ---------------------------

var (
	ffiOnce sync.Once
	ffiErr  error

	ffiNamespace          string
	ffiComponent          string
	ffiModel              string
	ffiOverlapScoreWeight float64
	ffiRouterTemperature  float64
	ffiKvBlockSize        uint32
	ffiWorkerID           int64

	runtimeInitialized bool

	// Boxed pipeline handle (owned on the Rust side, opaque here)
	pipeline      *C.struct_WorkerSelectionPipeline
	pipelineMutex sync.RWMutex
)

func loadDynamoConfig() {
	ffiNamespace = getEnvOrDefault("DYNAMO_NAMESPACE", "vllm-agg")
	ffiComponent = getEnvOrDefault("DYNAMO_COMPONENT", "backend")
	ffiModel = getEnvOrDefault("DYNAMO_MODEL", "Qwen/Qwen3-0.6B")
	ffiWorkerID = getEnvInt64OrDefault("DYNAMO_WORKER_ID", 1)

	ffiOverlapScoreWeight = getEnvFloatOrDefault("DYNAMO_OVERLAP_SCORE_WEIGHT", -1.0)
	ffiRouterTemperature = getEnvFloatOrDefault("DYNAMO_ROUTER_TEMPERATURE", -1.0)

	kvBlockSizeStr := os.Getenv("DYNAMO_KV_BLOCK_SIZE")
	if kvBlockSizeStr == "" {
		panic("DYNAMO_KV_BLOCK_SIZE is required and must match the model card's kv_cache_block_size")
	}
	var tmp int64
	if n, err := fmt.Sscanf(kvBlockSizeStr, "%d", &tmp); err != nil || n != 1 {
		panic(fmt.Sprintf("DYNAMO_KV_BLOCK_SIZE='%s' is not a valid integer", kvBlockSizeStr))
	}
	ffiKvBlockSize = uint32(tmp)
	if ffiKvBlockSize < 16 || ffiKvBlockSize > 8192 {
		panic(fmt.Sprintf("DYNAMO_KV_BLOCK_SIZE=%d outside [16,8192]", ffiKvBlockSize))
	}
	if (ffiKvBlockSize & (ffiKvBlockSize - 1)) != 0 {
		panic(fmt.Sprintf("DYNAMO_KV_BLOCK_SIZE=%d must be a power of 2", ffiKvBlockSize))
	}
	fmt.Printf("Dynamo KV Scorer: Loaded DYNAMO_KV_BLOCK_SIZE=%d\n", ffiKvBlockSize)
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func getEnvInt64OrDefault(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		var p int64
		if n, err := fmt.Sscanf(v, "%d", &p); err == nil && n == 1 {
			return p
		}
	}
	return def
}
func getEnvFloatOrDefault(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var p float64
		if n, err := fmt.Sscanf(v, "%f", &p); err == nil && n == 1 {
			return p
		}
	}
	return def
}
func getEnvBoolOrDefault(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(v) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return def
}

// initFFI: initialize runtime and create a persistent boxed pipeline.
func initFFI() error {
	ffiOnce.Do(func() {
		loadDynamoConfig()

		ns := C.CString(ffiNamespace)
		cm := C.CString(ffiComponent)
		model := C.CString(ffiModel)
		defer C.free(unsafe.Pointer(ns))
		defer C.free(unsafe.Pointer(cm))
		defer C.free(unsafe.Pointer(model))

		// Init Dynamo runtime
		if rc := C.dynamo_llm_init(ns, cm, C.int64_t(ffiWorkerID), C.uint32_t(ffiKvBlockSize)); rc != C.DYNAMO_OK {
			ffiErr = fmt.Errorf("dynamo_llm_init failed")
			return
		}
		runtimeInitialized = true

		// Create persistent pipeline
		pipelineMutex.Lock()
		defer pipelineMutex.Unlock()

		rc := C.dynamo_create_worker_selection_pipeline(
			ns,
			cm,
			model,
			C.bool(getEnvBoolOrDefault("DYNAMO_USE_KV_ROUTING", true)),
			C.double(getEnvFloatOrDefault("DYNAMO_BUSY_THRESHOLD", -1.0)),
			C.double(ffiOverlapScoreWeight),
			C.double(ffiRouterTemperature),
			C.bool(getEnvBoolOrDefault("DYNAMO_USE_KV_EVENTS", true)),
			C.bool(getEnvBoolOrDefault("DYNAMO_ROUTER_REPLICA_SYNC", true)),
			&pipeline,
		)
		if rc != C.DYNAMO_OK {
			ffiErr = fmt.Errorf("dynamo_create_worker_selection_pipeline failed")
			return
		}
	})
	return ffiErr
}

// --------------------------- scoring ---------------------------

func (k *KVAwareScorer) Score(
	ctx context.Context,
	cycle *schedtypes.CycleState,
	req *schedtypes.LLMRequest,
	pods []schedtypes.Pod,
) map[schedtypes.Pod]float64 {
	logger := log.FromContext(ctx)

	workerID, prefillWorkerID, tokenData, err := k.callDynamoRouter(ctx, req)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo call failed; proceeding without worker id")
	} else if workerID != "" {
		logger.V(logutil.DEFAULT).Info(
			"Dynamo router selected worker",
			"workerID", workerID,
			"prefillWorkerID", prefillWorkerID,
			"tokenDataCount", len(tokenData),
			"tokenData", tokenData,
		)
		cycle.Write(StateKeyWorkerInstanceID, stateString(workerID))
		if req.Headers == nil {
			req.Headers = map[string]string{}
		}
		req.Headers[WorkerIDHeader] = workerID

		// Set prefill worker ID if present
		if prefillWorkerID != "" {
			cycle.Write(StateKeyPrefillWorkerID, stateString(prefillWorkerID))
			req.Headers[PrefillWorkerIDHeader] = prefillWorkerID
		}

		if len(tokenData) > 0 {
			if req.Annotations == nil {
				req.Annotations = map[string]any{}
			}
			copied := make([]int64, len(tokenData))
			copy(copied, tokenData)
			req.Annotations[tokenDataAnnotationKey] = copied
		}
	}

	out := make(map[schedtypes.Pod]float64, len(pods))
	for _, p := range pods {
		out[p] = 1.0
	}
	return out
}

// --------------------------- router call (persistent only) ---------------------------

func (k *KVAwareScorer) callDynamoRouter(
	ctx context.Context,
	req *schedtypes.LLMRequest,
) (workerID string, prefillWorkerID string, tokenData []int64, err error) {
	logger := log.FromContext(ctx)

	if err := initFFI(); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "FFI init failed")
		return "", "", nil, err
	}
	if !runtimeInitialized {
		return "", "", nil, fmt.Errorf("dynamo runtime not initialized")
	}

	pipelineMutex.RLock()
	currentPipeline := pipeline
	pipelineMutex.RUnlock()

	if currentPipeline == nil {
		return "", "", nil, fmt.Errorf("dynamo worker selection pipeline not created")
	}

	// Build OpenAI-compatible JSON request
	requestBody := buildOpenAIRequest(req)
	requestJSON, jsonErr := json.Marshal(requestBody)
	if jsonErr != nil {
		logger.V(logutil.DEFAULT).Error(jsonErr, "Failed to marshal OpenAI request")
		return "", "", nil, fmt.Errorf("marshal OpenAI request: %w", jsonErr)
	}
	cRequestJSON := C.CString(string(requestJSON))
	defer C.free(unsafe.Pointer(cRequestJSON))

	// Output variables
	var cWorkerID C.int64_t
	var cPrefillWorkerID C.int64_t
	var cTokens *C.uint32_t
	var cTokenCount C.size_t
	var cAnnotatedJSON *C.char

	// Call the worker selection pipeline
	rc := C.dynamo_query_worker_selection_and_annotate(
		currentPipeline,
		cRequestJSON,
		&cWorkerID,
		&cPrefillWorkerID,
		&cTokens,
		&cTokenCount,
		&cAnnotatedJSON,
	)
	if rc != C.DYNAMO_OK {
		return "", "", nil, fmt.Errorf("dynamo_query_worker_selection_and_annotate failed")
	}

	// Copy tokens into Go memory and free C memory
	count := int(uintptr(cTokenCount))
	var tokens64 []int64
	if count > 0 && cTokens != nil {
		src := unsafe.Slice((*uint32)(unsafe.Pointer(cTokens)), count)
		tokens64 = make([]int64, count)
		for i := 0; i < count; i++ {
			tokens64[i] = int64(src[i])
		}
	}
	C.dynamo_free_worker_selection_result(cTokens, cTokenCount, cAnnotatedJSON)

	workerIDStr := fmt.Sprintf("%d", int64(cWorkerID))
	prefillWorkerIDStr := ""
	if int64(cPrefillWorkerID) != 0 {
		prefillWorkerIDStr = fmt.Sprintf("%d", int64(cPrefillWorkerID))
	}
	logger.V(logutil.DEFAULT).Info("Worker selection completed",
		"workerID", workerIDStr, "prefillWorkerID", prefillWorkerIDStr, "tokenCount", count)

	return workerIDStr, prefillWorkerIDStr, tokens64, nil
}

func buildOpenAIRequest(req *schedtypes.LLMRequest) map[string]any {
	requestBody := make(map[string]any)
	userText := "default prompt"
	if req != nil && strings.TrimSpace(req.Prompt) != "" {
		userText = req.Prompt
	}
	requestBody["messages"] = []map[string]any{{"role": "user", "content": userText}}
	if req != nil && strings.TrimSpace(req.TargetModel) != "" {
		requestBody["model"] = req.TargetModel
	} else {
		requestBody["model"] = ffiModel
	}
	requestBody["max_tokens"] = 1
	requestBody["temperature"] = 0.0
	requestBody["stream"] = true
	requestBody["nvext"] = map[string]any{
		"annotations": []string{"query_instance_id"},
	}
	return requestBody
}

// --------------------------- shutdown ---------------------------

func cleanupDynamo() error {
	pipelineMutex.Lock()
	defer pipelineMutex.Unlock()

	if pipeline != nil {
		if rc := C.dynamo_destroy_worker_selection_pipeline(pipeline); rc != C.DYNAMO_OK {
			fmt.Printf("Warning: dynamo_destroy_worker_selection_pipeline failed\n")
		}
		pipeline = nil
	}

	if runtimeInitialized {
		if rc := C.dynamo_llm_shutdown(); rc != C.DYNAMO_OK {
			return fmt.Errorf("dynamo_llm_shutdown failed")
		}
		runtimeInitialized = false
	}
	return nil
}

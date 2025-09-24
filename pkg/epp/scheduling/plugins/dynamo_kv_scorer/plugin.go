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
                                                               uint32_t **token_ids_out,
                                                               size_t *token_count_out,
                                                               char **annotated_request_json_out);

dynamo_llm_result_t dynamo_free_worker_selection_result(uint32_t *token_ids,
                                                        size_t token_count,
                                                        char *annotated_request_json);
*/
import "C"

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
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
	WorkerIDHeader           = "x-worker-instance-id"
	TokenDataHeader          = "x-epp-inject-nvext-token-data"
)

// --------------------------- config / env ---------------------------

var warmupOnce sync.Once
var warmupErr error

type params struct {
	FrontendURL string `json:"frontendURL"`
	TimeoutMS   int    `json:"timeoutMS"`
}

type stateString string

func (s stateString) Clone() schedtypes.StateData { return s }

type KVAwareScorer struct {
	typedName plugins.TypedName
	feURL     string
	feTimeout time.Duration
}

var _ plugins.Plugin = (*KVAwareScorer)(nil)
var _ framework.Scorer = (*KVAwareScorer)(nil)

func NewKVAwareScorer() *KVAwareScorer {
	return &KVAwareScorer{
		typedName: plugins.TypedName{Type: KVAwareScorerType, Name: PluginName},
		feURL:     "http://127.0.0.1:8000/v1/chat/completions",
		feTimeout: 10 * time.Second,
	}
}

func (k *KVAwareScorer) WithName(name string) *KVAwareScorer { k.typedName.Name = name; return k }
func (k *KVAwareScorer) WithFrontend(url string, timeout time.Duration) *KVAwareScorer {
	if url != "" {
		k.feURL = url
	}
	if timeout > 0 {
		k.feTimeout = timeout
	}
	return k
}

func KVAwareScorerFactory(name string, raw json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	p := params{}
	_ = json.Unmarshal(raw, &p)
	timeout := time.Duration(p.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	s := NewKVAwareScorer().WithName(name).WithFrontend(p.FrontendURL, timeout)

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
		return nil, fmt.Errorf("!!! Dynamo FFI init for the Router failed: %w", warmupErr)
	}

	return s, nil
}

func (k *KVAwareScorer) TypedName() plugins.TypedName { return k.typedName }

// --------------------------- SSE helpers (unchanged) ---------------------------

func (k *KVAwareScorer) callFrontEndForWorker(
	ctx context.Context,
	req *schedtypes.LLMRequest,
) (string, []int64, error) {
	logger := log.FromContext(ctx)

	feBody := buildFrontEndBodyFromLLMRequest(req)
	payload, err := json.Marshal(feBody)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd marshal failed")
		return "", nil, fmt.Errorf("marshal FrontEnd body: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, k.feTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, k.feURL, bytes.NewReader(payload))
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd request build failed")
		return "", nil, fmt.Errorf("build FrontEnd request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(httpReq)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd POST failed")
		return "", nil, fmt.Errorf("FrontEnd POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(resp.Body)
		logger.V(logutil.DEFAULT).Error(nil, "Dynamo FrontEnd non-2xx response",
			"status_code", resp.StatusCode, "response_body", string(errBody))
		return "", nil, fmt.Errorf("Dynamo FrontEnd error: %d body=%s", resp.StatusCode, string(errBody))
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(ct, "text/event-stream") {
		logger.V(logutil.DEFAULT).Error(nil, "Unexpected non-SSE response")
		return "", nil, fmt.Errorf("unexpected non-SSE response (Content-Type=%q)", resp.Header.Get("Content-Type"))
	}

	reader := bufio.NewReader(resp.Body)
	workerID, tokenData, perr := parseSelectionFromSSE(ctx, reader)
	if perr != nil {
		return "", nil, perr
	}
	return workerID, tokenData, nil
}

func buildFrontEndBodyFromLLMRequest(req *schedtypes.LLMRequest) map[string]any {
	feBody := make(map[string]any, 8)
	userText := ""
	if req != nil && strings.TrimSpace(req.Prompt) != "" {
		userText = req.Prompt
	}
	feBody["messages"] = []map[string]any{{"role": "user", "content": userText}}
	if req != nil && strings.TrimSpace(req.TargetModel) != "" {
		feBody["model"] = req.TargetModel
	}
	feBody["stream"] = true
	feBody["max_tokens"] = 1
	feBody["temperature"] = 0.0
	return feBody
}

func parseSelectionFromSSE(ctx context.Context, reader *bufio.Reader) (string, []int64, error) {
	logger := log.FromContext(ctx)
	var (
		eventName  string
		dataBuf    strings.Builder
		commentBuf strings.Builder
		gotWID     string
		gotTD      []int64
	)
	var rawBuf strings.Builder

	flushEvent := func() (bool, error) {
		data := strings.TrimSpace(dataBuf.String())
		comment := strings.TrimSpace(commentBuf.String())
		dataBuf.Reset()
		commentBuf.Reset()

		if data == "[DONE]" || comment == "[DONE]" {
			logger.V(logutil.DEFAULT).Info("SSE stream DONE")
			logger.V(logutil.DEFAULT).Info("SSE raw stream", "raw", rawBuf.String())
			if gotWID != "" && len(gotTD) == 0 {
				logger.V(logutil.DEFAULT).Info("SSE DONE: worker_instance_id present, token_data missing")
			}
			return true, nil
		}

		if eventName == "worker_instance_id" {
			candidate := data
			if candidate == "" {
				candidate = comment
			}
			if candidate != "" {
				var s string
				if json.Unmarshal([]byte(candidate), &s) == nil && s != "" {
					gotWID = s
					return false, nil
				}
				clean := strings.Trim(candidate, "\"")
				if clean != "" && clean != "[DONE]" {
					gotWID = clean
					return false, nil
				}
			}
		}

		if eventName == "token_data" {
			candidate := data
			if candidate == "" {
				candidate = comment
			}
			if candidate != "" {
				if arr := toInt64SliceJSON(candidate); len(arr) > 0 {
					gotTD = arr
					return false, nil
				}
			}
		}

		if data != "" {
			var msg map[string]any
			if json.Unmarshal([]byte(data), &msg) == nil {
				if wid, ok := msg["worker_instance_id"].(string); ok && wid != "" {
					gotWID = wid
				}
				if ann, ok := msg["annotations"].(map[string]any); ok {
					if wid, ok := ann["worker_instance_id"].(string); ok && wid != "" {
						gotWID = wid
					}
				}
				if td, ok := msg["token_data"]; ok {
					if arr := toInt64Slice(td); len(arr) > 0 {
						gotTD = arr
					}
				} else if nv, ok := msg["nvext"].(map[string]any); ok {
					if td, ok := nv["token_data"]; ok {
						if arr := toInt64Slice(td); len(arr) > 0 {
							gotTD = arr
						}
					}
				}
			}
		}
		return false, nil
	}

	for {
		line, err := reader.ReadString('\n')
		rawBuf.WriteString(line)
		if err != nil {
			if err == io.EOF {
				_, _ = flushEvent()
				logger.V(logutil.DEFAULT).Info("SSE raw stream (EOF)", "raw", rawBuf.String())
				if gotWID != "" && len(gotTD) == 0 {
					logger.V(logutil.DEFAULT).Info("EOF: worker_instance_id present, token_data missing")
				}
				if gotWID != "" || len(gotTD) > 0 {
					return gotWID, gotTD, nil
				}
				logger.V(logutil.DEFAULT).Error(nil, "EOF before selection fields present")
				return "", nil, fmt.Errorf("selection not found in SSE stream (EOF)")
			}
			logger.V(logutil.DEFAULT).Error(err, "SSE read error")
			return "", nil, fmt.Errorf("sse read error: %w", err)
		}

		l := strings.TrimRight(line, "\r\n")
		if l == "" {
			if done, _ := flushEvent(); done {
				if gotWID != "" && len(gotTD) == 0 {
					logger.V(logutil.DEFAULT).Info("SSE DONE: worker_instance_id present, token_data missing")
				}
				return gotWID, gotTD, nil
			}
			eventName = ""
			continue
		}

		if strings.HasPrefix(l, ":") {
			commentLine := strings.TrimSpace(l[1:])
			if commentBuf.Len() > 0 {
				commentBuf.WriteByte('\n')
			}
			commentBuf.WriteString(commentLine)
			continue
		}

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
			}
		}
	}
}

func encodeTokenData(tokens []int64) string {
	b, _ := json.Marshal(tokens)
	return base64.StdEncoding.EncodeToString(b)
}

func toInt64Slice(v any) []int64 {
	xs, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(xs))
	for _, it := range xs {
		switch n := it.(type) {
		case float64:
			out = append(out, int64(n))
		case int64:
			out = append(out, n)
		case json.Number:
			if i, err := n.Int64(); err == nil {
				out = append(out, i)
			}
		}
	}
	return out
}

func toInt64SliceJSON(s string) []int64 {
	var arr []int64
	if err := json.Unmarshal([]byte(s), &arr); err == nil && len(arr) > 0 {
		return arr
	}
	var inner string
	if err := json.Unmarshal([]byte(s), &inner); err == nil && inner != "" {
		var arr2 []int64
		if err := json.Unmarshal([]byte(inner), &arr2); err == nil && len(arr2) > 0 {
			return arr2
		}
	}
	unquoted := strings.Trim(s, "\"")
	if unquoted != s {
		var arr3 []int64
		if err := json.Unmarshal([]byte(unquoted), &arr3); err == nil && len(arr3) > 0 {
			return arr3
		}
	}
	return nil
}

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

		// 1) runtime
		if rc := C.dynamo_llm_init(ns, cm, C.int64_t(ffiWorkerID), C.uint32_t(ffiKvBlockSize)); rc != C.DYNAMO_OK {
			ffiErr = fmt.Errorf("dynamo_llm_init failed")
			return
		}
		runtimeInitialized = true

		// 2) create persistent pipeline
		pipelineMutex.Lock()
		defer pipelineMutex.Unlock()

		rc := C.dynamo_create_worker_selection_pipeline(
			ns,
			cm,
			model,
			C.bool(true),                    // use_kv_routing
			C.double(-1.0),                  // busy_threshold (default)
			C.double(ffiOverlapScoreWeight), // overlap_score_weight (neg = default)
			C.double(ffiRouterTemperature),  // router_temperature (neg = default)
			C.bool(getEnvBoolOrDefault("DYNAMO_USE_KV_EVENTS", true)),
			C.bool(getEnvBoolOrDefault("DYNAMO_ROUTER_REPLICA_SYNC", false)),
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

	workerID, tokenData, err := k.callDynamoRouter(ctx, req)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo call failed; proceeding without worker id")
	} else if workerID != "" {
		logger.V(logutil.DEFAULT).Info(
			"Dynamo router selected worker",
			"workerID", workerID,
			"tokenDataCount", len(tokenData),
			"tokenData", tokenData,
		)
		cycle.Write(StateKeyWorkerInstanceID, stateString(workerID))
		if req.Headers == nil {
			req.Headers = map[string]string{}
		}
		req.Headers[WorkerIDHeader] = workerID
		if len(tokenData) > 0 {
			if req.Headers == nil {
				req.Headers = map[string]string{}
			}
			req.Headers[TokenDataHeader] = encodeTokenData(tokenData)
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
) (string, []int64, error) {
	logger := log.FromContext(ctx)

	if err := initFFI(); err != nil {
		logger.V(logutil.DEFAULT).Error(err, "FFI init failed")
		return "", nil, err
	}
	if !runtimeInitialized {
		return "", nil, fmt.Errorf("dynamo runtime not initialized")
	}

	pipelineMutex.RLock()
	currentPipeline := pipeline
	pipelineMutex.RUnlock()

	if currentPipeline == nil {
		return "", nil, fmt.Errorf("dynamo worker selection pipeline not created")
	}

	// Build OpenAI-compatible JSON request
	requestBody := buildOpenAIRequest(req)
	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Failed to marshal OpenAI request")
		return "", nil, fmt.Errorf("marshal OpenAI request: %w", err)
	}
	cRequestJSON := C.CString(string(requestJSON))
	defer C.free(unsafe.Pointer(cRequestJSON))

	// Output variables
	var cWorkerID C.int64_t
	var cTokens *C.uint32_t
	var cTokenCount C.size_t
	var cAnnotatedJSON *C.char

	// Call the worker selection pipeline
	rc := C.dynamo_query_worker_selection_and_annotate(
		currentPipeline,
		cRequestJSON,
		&cWorkerID,
		&cTokens,
		&cTokenCount,
		&cAnnotatedJSON,
	)
	if rc != C.DYNAMO_OK {
		return "", nil, fmt.Errorf("!!! dynamo_query_worker_selection_and_annotate failed")
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

	workerID := fmt.Sprintf("%d", int64(cWorkerID))
	logger.V(logutil.DEFAULT).Info("Worker selection completed",
		"workerID", workerID, "tokenCount", count)

	return workerID, tokens64, nil
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

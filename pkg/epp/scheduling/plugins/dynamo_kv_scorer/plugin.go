package dynamo_kv_scorer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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
)

type params struct {
	FrontendURL string `json:"frontendURL"`
	TimeoutMS   int    `json:"timeoutMS"`
}

// tiny wrapper so we can store a string in CycleState
type stateString string

func (s stateString) Clone() schedtypes.StateData { return s }

type KVAwareScorer struct {
	typedName plugins.TypedName
	feURL     string
	feTimeout time.Duration
}

// compile-time assertions
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
	return NewKVAwareScorer().WithName(name).WithFrontend(p.FrontendURL, timeout), nil
}

func (k *KVAwareScorer) TypedName() plugins.TypedName { return k.typedName }

func (k *KVAwareScorer) Score(
	ctx context.Context,
	cycle *schedtypes.CycleState,
	req *schedtypes.LLMRequest,
	pods []schedtypes.Pod,
) map[schedtypes.Pod]float64 {
	logger := log.FromContext(ctx)

	workerID, err := k.callFrontEndForWorker(ctx, req)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "FrontEnd call failed; proceeding without worker id")
	} else if workerID != "" {
		cycle.Write(StateKeyWorkerInstanceID, stateString(workerID))
		if req.Headers == nil {
			req.Headers = map[string]string{}
		}
		req.Headers[WorkerIDHeader] = workerID
	}

	// neutral/uniform scores – only your scorer runs in the profile, so this “wins”
	out := make(map[schedtypes.Pod]float64, len(pods))
	for _, p := range pods {
		out[p] = 1.0
	}
	return out
}

// Call the Dynamo FrontEnd and extract worker_instance_id via SSE.
func (k *KVAwareScorer) callFrontEndForWorker(
	ctx context.Context,
	req *schedtypes.LLMRequest,
) (string, error) {
	logger := log.FromContext(ctx)

	feBody := buildFrontEndBodyFromLLMRequest(req)
	payload, err := json.Marshal(feBody)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd marshal failed")
		return "", fmt.Errorf("marshal FrontEnd body: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, k.feTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, k.feURL, bytes.NewReader(payload))
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd request build failed")
		return "", fmt.Errorf("build FrontEnd request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(httpReq)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Dynamo FrontEnd POST failed")
		return "", fmt.Errorf("FrontEnd POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(resp.Body)
		logger.V(logutil.DEFAULT).Error(nil, "Dynamo FrontEnd non-2xx response",
			"status_code", resp.StatusCode, "response_body", string(errBody))
		return "", fmt.Errorf("Dynamo FrontEnd error: %d body=%s", resp.StatusCode, string(errBody))
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(ct, "text/event-stream") {
		logger.V(logutil.DEFAULT).Error(nil, "Unexpected non-SSE response")
		return "", fmt.Errorf("unexpected non-SSE response (Content-Type=%q)", resp.Header.Get("Content-Type"))
	}

	// Parse SSE: expect `event: worker_instance_id`, a quoted id in a comment or data, and `data: [DONE]`
	reader := bufio.NewReader(resp.Body)
	workerID, perr := parseWorkerIDFromSSE(ctx, reader)
	if perr != nil {
		return "", perr
	}
	return workerID, nil
}

// Build the exact body we send to the FrontEnd, only from LLMRequest (no header merging).
func buildFrontEndBodyFromLLMRequest(req *schedtypes.LLMRequest) map[string]any {
	feBody := make(map[string]any, 8)

	// We call /v1/chat/completions so must provide messages
	userText := ""
	if req != nil && strings.TrimSpace(req.Prompt) != "" {
		userText = req.Prompt
	}
	feBody["messages"] = []map[string]any{
		{"role": "user", "content": userText},
	}

	if req != nil && strings.TrimSpace(req.TargetModel) != "" {
		feBody["model"] = req.TargetModel
	}

	// Force SSE so we can parse worker_instance_id
	feBody["stream"] = true

	feBody["max_tokens"] = 1
	feBody["temperature"] = 0.0

	// Ask the Dynamo to include worker id
	feBody["nvext"] = map[string]any{
		"annotations": []string{"query_instance_id"},
	}

	return feBody
}

// parseWorkerIDFromSSE scans an SSE stream for a worker_instance_id.
// Expected pattern:
//
//	event: worker_instance_id
//	: "8303679623149182543"
//	data: [DONE]
//
// Also supports JSON in data lines with either top-level worker_instance_id
// or annotations.worker_instance_id.
func parseWorkerIDFromSSE(ctx context.Context, reader *bufio.Reader) (string, error) {
	logger := log.FromContext(ctx)

	var (
		eventName  string
		dataBuf    strings.Builder // accumulates "data:" lines for one event
		commentBuf strings.Builder // accumulates ":" comment lines
	)

	flushEvent := func() (string, bool, error) {
		data := strings.TrimSpace(dataBuf.String())
		comment := strings.TrimSpace(commentBuf.String())
		dataBuf.Reset()
		commentBuf.Reset()

		// [DONE] ends the stream
		if data == "[DONE]" || comment == "[DONE]" {
			logger.V(logutil.DEFAULT).Info("SSE stream DONE")
			return "", true, nil
		}

		// Prefer the named event
		if eventName == "worker_instance_id" {
			candidate := data
			if candidate == "" {
				candidate = comment
			}
			if candidate != "" {
				// Try JSON string
				var s string
				if json.Unmarshal([]byte(candidate), &s) == nil && s != "" {
					logger.V(logutil.VERBOSE).Info("worker_instance_id extracted from named event", "worker_instance_id", s)
					return s, false, nil
				}
				// Fallback: strip quotes
				clean := strings.Trim(candidate, "\"")
				if clean != "" && clean != "[DONE]" {
					logger.V(logutil.DEFAULT).Info("worker_instance_id extracted (raw) from named event", "worker_instance_id", clean)
					return clean, false, nil
				}
			}
		}

		// Generic JSON in data:
		if data != "" {
			var msg map[string]any
			if json.Unmarshal([]byte(data), &msg) == nil {
				if wid, ok := msg["worker_instance_id"].(string); ok && wid != "" {
					logger.V(logutil.DEFAULT).Info("worker_instance_id found in SSE payload root", "worker_instance_id", wid)
					return wid, false, nil
				}
				if ann, ok := msg["annotations"].(map[string]any); ok {
					if wid, ok := ann["worker_instance_id"].(string); ok && wid != "" {
						logger.V(logutil.DEFAULT).Info("worker_instance_id found in SSE annotations", "worker_instance_id", wid)
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
				// Flush any pending event on EOF
				if wid, done, _ := flushEvent(); wid != "" {
					return wid, nil
				} else if done {
					return "", fmt.Errorf("worker_instance_id not found before DONE")
				}
				logger.V(logutil.DEFAULT).Error(nil, "EOF before worker_instance_id")
				return "", fmt.Errorf("worker_instance_id not found in SSE stream (EOF)")
			}
			logger.V(logutil.DEFAULT).Error(err, "SSE read error")
			return "", fmt.Errorf("sse read error: %w", err)
		}

		l := strings.TrimRight(line, "\r\n")
		if l == "" {
			// End of current event; process it
			if wid, done, _ := flushEvent(); wid != "" {
				return wid, nil
			} else if done {
				return "", fmt.Errorf("worker_instance_id not found before DONE")
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

		// "field: value"
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
				// ignore id, retry, etc.
			}
		}
	}
}

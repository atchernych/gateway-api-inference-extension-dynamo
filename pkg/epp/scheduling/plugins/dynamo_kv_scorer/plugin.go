package dynamo_kv_scorer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
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
	TokenDataHeader          = "x-epp-inject-nvext-token-data"
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

	workerID, tokenData, err := k.callFrontEndForWorker(ctx, req)
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "FrontEnd call failed; proceeding without worker id")
	} else if workerID != "" {
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

	// Parse SSE: expect `event: worker_instance_id`, a quoted id in a comment or data, and `data: [DONE]`
	reader := bufio.NewReader(resp.Body)
	workerID, tokenData, perr := parseSelectionFromSSE(ctx, reader)
	if perr != nil {
		return "", nil, perr
	}
	return workerID, tokenData, nil
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

// This function scans an SSE stream for a worker_instance_id and token_data.
// Expected pattern:
//
//	event: worker_instance_id
//	: "8303679623149182543"
//	data: [DONE]

// or with tokens:
// event: worker_instance_id\n: \"8228244551594056720\"\n\n
// event: token_data\n: \"[151644,872,198,151644,872,198,14990,151645,198,151645,198,151644,77091,198]\
// "\n\ndata: [DONE]\n\n"
// Also supports JSON in data lines with either top-level worker_instance_id
// or annotations.worker_instance_id.
func parseSelectionFromSSE(ctx context.Context, reader *bufio.Reader) (string, []int64, error) {
	logger := log.FromContext(ctx)

	var (
		eventName  string
		dataBuf    strings.Builder // accumulates "data:" lines for one event
		commentBuf strings.Builder // accumulates ":" comment lines
		gotWID     string
		gotTD      []int64
	)

	// collect the exact SSE bytes for debugging
	var rawBuf strings.Builder

	flushEvent := func() (bool, error) {
		data := strings.TrimSpace(dataBuf.String())
		comment := strings.TrimSpace(commentBuf.String())
		dataBuf.Reset()
		commentBuf.Reset()

		// [DONE] ends the stream
		if data == "[DONE]" || comment == "[DONE]" {
			logger.V(logutil.DEFAULT).Info("SSE stream DONE")
			logger.V(logutil.DEFAULT).Info("SSE raw stream", "raw", rawBuf.String())
			if gotWID != "" && len(gotTD) == 0 {
				logger.V(logutil.DEFAULT).Info("SSE DONE: worker_instance_id present, token_data missing")
			}
			return true, nil
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
					gotWID = s
					return false, nil
				}
				// Fallback: strip quotes
				clean := strings.Trim(candidate, "\"")
				if clean != "" && clean != "[DONE]" {
					logger.V(logutil.DEFAULT).Info("worker_instance_id extracted (raw) from named event", "worker_instance_id", clean)
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
					logger.V(logutil.DEFAULT).Info("token_data extracted from named event", "count", len(arr))
					return false, nil
				}
			}
		}
		// Generic JSON in data:
		if data != "" {
			var msg map[string]any
			if json.Unmarshal([]byte(data), &msg) == nil {
				if wid, ok := msg["worker_instance_id"].(string); ok && wid != "" {
					logger.V(logutil.DEFAULT).Info("worker_instance_id found in SSE payload root", "worker_instance_id", wid)
					gotWID = wid
				}
				if ann, ok := msg["annotations"].(map[string]any); ok {
					if wid, ok := ann["worker_instance_id"].(string); ok && wid != "" {
						logger.V(logutil.DEFAULT).Info("worker_instance_id found in SSE annotations", "worker_instance_id", wid)
						gotWID = wid
					}
				}
				if td, ok := msg["token_data"]; ok {
					if arr := toInt64Slice(td); len(arr) > 0 {
						gotTD = arr
						logger.V(logutil.DEFAULT).Info("token_data found in SSE payload root", "count", len(arr))
					}
				} else if nv, ok := msg["nvext"].(map[string]any); ok {
					if td, ok := nv["token_data"]; ok {
						if arr := toInt64Slice(td); len(arr) > 0 {
							gotTD = arr
							logger.V(logutil.DEFAULT).Info("token_data found in SSE nvext", "count", len(arr))
						}
					}
				}
			}
		}
		return false, nil
	}

	for {
		line, err := reader.ReadString('\n')
		// capture the raw stream as-is for debugging
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
			// End of current event.
			if done, _ := flushEvent(); done {
				if gotWID != "" && len(gotTD) == 0 {
					logger.V(logutil.DEFAULT).Info("SSE DONE: worker_instance_id present, token_data missing")
				}
				return gotWID, gotTD, nil
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

// encodeTokenData turns []int64 into base64(JSON array) for a safe header value.
func encodeTokenData(tokens []int64) string {
	b, _ := json.Marshal(tokens)
	return base64.StdEncoding.EncodeToString(b)
}

// Accepts interface{} from a parsed JSON map
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

// Accepts raw JSON (string) for events like:
// event: worker_instance_id\n: \"8228244551594056720\"\n\n
// event: token_data\n: \"[151644,872,198,151644,872,198,14990,151645,198,151645,198,151644,77091,198]\
// "\n\ndata: [DONE]\n\n"
// replaces the old toInt64SliceJSON
func toInt64SliceJSON(s string) []int64 {
	// case 1: direct JSON array
	var arr []int64
	if err := json.Unmarshal([]byte(s), &arr); err == nil && len(arr) > 0 {
		return arr
	}
	// case 2: s is a JSON string that itself contains a JSON array
	var inner string
	if err := json.Unmarshal([]byte(s), &inner); err == nil && inner != "" {
		var arr2 []int64
		if err := json.Unmarshal([]byte(inner), &arr2); err == nil && len(arr2) > 0 {
			return arr2
		}
	}
	// case 3: strip quotes and try once more
	unquoted := strings.Trim(s, "\"")
	if unquoted != s {
		var arr3 []int64
		if err := json.Unmarshal([]byte(unquoted), &arr3); err == nil && len(arr3) > 0 {
			return arr3
		}
	}
	return nil
}

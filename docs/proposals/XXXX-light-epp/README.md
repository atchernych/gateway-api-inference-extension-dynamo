# Light EPP: Minimal Reference Endpoint Picker

Author(s): @atchernych

## Proposal Status

***Provisional***

## Related Issues

- [#2430 — Lighter EPP discussion](https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/2430)
- [004 — Endpoint Picker Protocol](../004-endpoint-picker-protocol/README.md)
- [0683 — EPP Architecture Proposal](../0683-epp-architecture-proposal/README.md)

## Summary

This proposal introduces a **Light EPP** (`pkg/epp-light/`) — a minimal, API-focused Endpoint Picker implementation that separates the Endpoint Picker Protocol from the production scheduling logic. It defines a single `EndpointPicker` interface that anyone can implement to build their own EPP, while the framework handles all Envoy ext-proc protocol details, InferencePool CRD discovery, and pod lifecycle management.

The Light EPP is intended to serve as:

1. A **reference implementation** for the Endpoint Picker Protocol (proposal 004)
2. A **conformance testing target** for gateway providers
3. A **starting point** for third-party EPP implementations

## Goals

- Define a clean, single-method `EndpointPicker` interface that decouples endpoint selection from protocol handling
- Fully implement the [Endpoint Picker Protocol](../004-endpoint-picker-protocol/README.md) (subset filtering, destination metadata, fallback endpoints)
- Support the stable `v1` InferencePool CRD for pod discovery
- Provide a default random-selection implementation as a reference
- Keep the codebase minimal (~12 files) and self-contained with zero imports from `pkg/epp/`

## Non-Goals

- Replace the full `pkg/epp/` for production workloads
- Support InferenceObjective (priority/fairness) or InferenceModelRewrite (traffic splitting) CRDs
- Implement scheduling frameworks, flow control, or plugin systems
- Provide Prometheus metrics or observability instrumentation (can be added later)
- Support the experimental `v1alpha2` InferencePool API

## Motivation

The current `pkg/epp/` is a full-featured, production-grade system but it creates a high barrier for:

- **Gateway providers** who need a conformance target for the EPP protocol
- **Third-party implementors** who want to build custom endpoint selection without understanding 30+ files of internal machinery
- **Testing** where a simple, predictable EPP is sufficient

As discussed in [#2430](https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/2430), the EPP API/protocol should remain in Kubernetes governance while production selection logic moves to specialized projects (e.g., llm-d). The Light EPP embodies this separation.

## Proposal

### Architecture

The Light EPP decomposes the EPP into two layers:

```
                    +-----------------------------------------+
                    |           Envoy (ext-proc client)        |
                    +-------------------+---------------------+
                                        |
                    +-------------------v---------------------+
                    |          Protocol Layer (framework)      |
                    |                                         |
                    |  - Ext-proc Process loop (gRPC)         |
                    |  - Request/response state machine       |
                    |  - EPP metadata generation              |
                    |  - Subset filtering                     |
                    |  - InferencePool + Pod reconcilers      |
                    +-------------------+---------------------+
                                        |
                           EndpointPicker.Pick()
                                        |
                    +-------------------v---------------------+
                    |          Selection Layer (pluggable)     |
                    |                                         |
                    |  Implement your own logic:              |
                    |  - Random (default)                     |
                    |  - Model-aware routing                  |
                    |  - Prefix-cache affinity                |
                    |  - Latency-based scoring                |
                    |  - Any custom algorithm                 |
                    +-----------------------------------------+
```

### Core Interface

The central abstraction is a single interface with one method:

```go
type EndpointPicker interface {
    Pick(ctx context.Context, req *RequestInfo, endpoints []Endpoint) (*PickResult, error)
}
```

Where:

```go
type Endpoint struct {
    Address string            // Pod IP
    Port    string            // Target port
    Name    string            // Pod name (namespace/name)
    Labels  map[string]string // Pod labels
}

type RequestInfo struct {
    Headers         map[string]string // HTTP request headers
    Body            []byte            // Raw request body (nil for GET)
    Model           string            // Model name from body, if present
    CandidateSubset []string          // From x-gateway-destination-endpoint-subset
}

type PickResult struct {
    Endpoint  string   // Primary endpoint (ip:port)
    Fallbacks []string // Optional fallback endpoints (ip:port)
}
```

**Design properties:**

- **Rich input** — `RequestInfo` provides headers, raw body, extracted model name, and the candidate subset, giving implementors everything they need for intelligent routing decisions
- **Rich endpoint data** — `Endpoint` includes pod labels, enabling label-based routing without a scheduling framework
- **Protocol compliance is handled** — subset filtering, metadata generation, and ext-proc state management are the framework's concern, not the implementor's
- **Flat types** — `Endpoint` is a simple struct, not an interface with metrics/attributes/factory indirection

### What Changed vs. `pkg/epp/`

The current EPP request flow is a 10-step orchestration pipeline:

```
Request → parse → getObjective → admit → locateCandidates → prepareData
        → admissionPlugins → schedule → preRequest → modelRewrite → respond
```

The Light EPP collapses this to 4 steps:

```
Request → extractModel → resolveSubset+filterEndpoints → picker.Pick → respond
```

| Current `pkg/epp/` | Light EPP | Rationale |
|---|---|---|
| `Director` orchestrator | Direct `picker.Pick()` call | No need for multi-step orchestration |
| `Scheduler` with profiles/filters/scorers | `EndpointPicker` interface | Selection algorithm is the implementor's concern |
| `Parser` interface + `LLMRequestBody` | Simple JSON `model` extraction | Raw body available in `RequestInfo.Body` for custom parsing |
| InferenceObjective reconciler + priority | Removed | Out of scope |
| InferenceModelRewrite reconciler + traffic split | Removed | Out of scope |
| Flow control (saturation, fairness, queuing) | Removed | Implementation concern |
| Data layer (polling, notifications, extractors) | Removed | Implementation concern |
| Plugin registry + DAG validation | Removed | No plugin system needed |
| `EndpointFactory` + metrics goroutines | Simple `Endpoint` struct | No background metrics collection |
| `fwkdl.Endpoint` interface | `Endpoint` value type | No interface indirection needed |

### Package Structure

```
pkg/epp-light/
    picker.go              — EndpointPicker interface, Endpoint/RequestInfo/PickResult types
    picker_random.go       — Default random picker (reference implementation)
    metadata.go            — EPP protocol constants (proposal 004)
    datastore.go           — Simplified datastore (pool + pods only)
    server.go              — Ext-proc StreamingServer with Process loop
    request.go             — Request handling, metadata generation, subset filtering
    response.go            — Response handling
    controller/
        pool.go            — InferencePool v1 reconciler
        pod.go             — Pod reconciler
    server/
        runner.go          — ExtProcServerRunner (gRPC wiring)
        options.go         — Minimal CLI flags
cmd/epp-light/
    main.go                — Entrypoint with RandomPicker
```

12 files total, versus dozens in `pkg/epp/`.

### Dependency Isolation

The Light EPP has **zero imports from `pkg/epp/`**:

| Dependency | Source | Usage |
|---|---|---|
| `api/v1` | Shared CRD types | InferencePool type definitions |
| `pkg/common/envoy` | Shared utilities | Envoy helpers (headers, metadata, chunking) |
| `pkg/common/error` | Shared utilities | Error types and `BuildErrResponse` |
| `pkg/common/request` | Shared utilities | `RequestIdHeaderKey` constant |
| `internal/runnable` | Infrastructure | `GRPCServer`, `NoLeaderElection` |
| `internal/tls` | Infrastructure | TLS certificate handling |

The 4 EPP protocol constants (`SubsetFilterNamespace`, `SubsetFilterKey`, `DestinationEndpointNamespace`, `DestinationEndpointKey`) are copied into `metadata.go` rather than imported from `pkg/epp/metadata`, ensuring the two packages can evolve independently.

### Datastore Simplifications

| `pkg/epp/datastore` | `pkg/epp-light/datastore` |
|---|---|
| `EndpointFactory` with background goroutines | Direct `Endpoint` struct creation |
| `fwkdl.Endpoint` interface with `GetMetrics()`, `GetAttributes()` | Simple `Endpoint` value struct |
| `ObjectiveSet/Get/Delete/GetAll` | Removed |
| `ModelRewriteSet/Get/Delete/GetAll` | Removed |
| `modelServerMetricsPort` | Removed |
| `parentCtx` for goroutine lifecycle | Not needed (no goroutines) |

Retained: pool set/get with pod resync, pod CRUD via `sync.Map`, rank-based endpoint naming for multi-port, `activePortsAnnotation` support, label selector matching.

### Usage Example

Building a custom EPP requires implementing one interface:

```go
package main

import (
    "context"
    "fmt"
    "os"

    ctrl "sigs.k8s.io/controller-runtime"
    epplight "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light"
    "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light/server"
)

// ModelAwarePicker routes requests to endpoints whose pods are labeled
// with the requested model name.
type ModelAwarePicker struct{}

func (p *ModelAwarePicker) Pick(
    ctx context.Context,
    req *epplight.RequestInfo,
    endpoints []epplight.Endpoint,
) (*epplight.PickResult, error) {
    // Route to endpoints labeled with the target model.
    for _, ep := range endpoints {
        if ep.Labels["model"] == req.Model {
            return &epplight.PickResult{
                Endpoint: ep.Address + ":" + ep.Port,
            }, nil
        }
    }
    // Fallback: first available endpoint.
    if len(endpoints) > 0 {
        ep := endpoints[0]
        return &epplight.PickResult{
            Endpoint: ep.Address + ":" + ep.Port,
        }, nil
    }
    return nil, fmt.Errorf("no endpoints available for model %q", req.Model)
}

func main() {
    // Wire the custom picker into the light EPP server.
    opts := server.NewOptions()
    opts.AddFlags(pflag.CommandLine)
    pflag.Parse()

    mgr, _ := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
    ds := epplight.NewDatastore()

    runner := &server.ExtProcServerRunner{
        GRPCPort:      opts.GRPCPort,
        PoolNamespace: opts.PoolNamespace,
        PoolName:      opts.PoolName,
        Datastore:     ds,
        Picker:        &ModelAwarePicker{},
    }
    runner.SetupWithManager(mgr)
    mgr.Add(runner.AsRunnable(logger))
    mgr.Start(ctrl.SetupSignalHandler())
}
```

## Protocol Conformance

The Light EPP fully implements the [Endpoint Picker Protocol](../004-endpoint-picker-protocol/README.md):

| Protocol Requirement | Implementation |
|---|---|
| Set `x-gateway-destination-endpoint` header | `request.go:generateRequestHeaders()` |
| Set `envoy.lb` dynamic metadata | `request.go:generateDestinationMetadata()` |
| Respect `envoy.lb.subset_hint` / `x-gateway-destination-endpoint-subset` | `request.go:extractCandidateSubset()` + `filterEndpointsBySubset()` |
| Support multiple endpoints (fallback) | `PickResult.Fallbacks` joined with `,` |
| Return 503 when no endpoints available | `server.go:handleRequestBody()` |
| Header and metadata values must match | Single `targetEndpoint` string used for both |

## Alternatives Considered

### 1. Define EndpointPicker as a plugin within the existing framework

The existing plugin framework (`framework/interface/scheduling/plugins.go`) defines Filter, Scorer, and Picker as separate interfaces composed into SchedulerProfiles. This is powerful but forces implementors to understand the profile/filter/scorer/picker pipeline. A single `Pick` method is a lower abstraction barrier.

### 2. Use `fwkdl.Endpoint` interface instead of a flat struct

The existing `fwkdl.Endpoint` interface requires `GetMetrics()`, `GetAttributes()`, `UpdateMetrics()`, and the `EndpointFactory` abstraction. This pulls in the data layer framework. A simple struct with Address, Port, Name, Labels is sufficient for routing decisions and avoids the coupling.

## Testing

### Unit Tests

- `picker_random_test.go` — Random selection, empty list error, distribution across endpoints
- `datastore_test.go` — Pool set/get, pod CRUD, label matching, endpoint listing, active ports
- `server_test.go` — Ext-proc Process loop with mock stream (header-only, body, subset filtering, no-endpoints)
- `request_test.go` — `extractModelFromBody`, `extractCandidateSubset`, `filterEndpointsBySubset`, metadata generation
- `controller/*_test.go` — Pool and pod reconcilers with fake k8s client

### Integration Test

- Start full Light EPP against a fake k8s client, send ext-proc requests via gRPC client, verify endpoint selection appears in both headers and dynamic metadata

### Conformance Checks

Per proposal 004:

- `x-gateway-destination-endpoint` header set on every successful response
- `envoy.lb` dynamic metadata set with matching endpoint value
- Subset filtering respects `envoy.lb.subset_hint`
- 503 returned when no endpoints available
- 503 returned when candidate subset matches no available endpoints

### Build Verification

```bash
go build ./pkg/epp-light/...
go build ./cmd/epp-light/...
go vet ./pkg/epp-light/... ./cmd/epp-light/...
```

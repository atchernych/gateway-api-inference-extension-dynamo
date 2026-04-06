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
3. A **starting point** for third-party EPP implementations in any language

## Goals

- Define a clean, single-method `EndpointPicker` interface that decouples endpoint selection from protocol handling
- Fully implement the [Endpoint Picker Protocol](../004-endpoint-picker-protocol/README.md) (subset filtering, destination metadata, fallback endpoints)
- Support the stable `v1` InferencePool CRD for pod discovery
- Provide a default random-selection implementation as a reference
- **Enable cross-language implementations** via a gRPC `EndpointPickerService` protobuf definition, so Rust, Python, C++, or any language can implement custom endpoint selection
- Keep the codebase minimal (~17 files) and self-contained with zero imports from `pkg/epp/`

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

### Cross-Language Support via gRPC Picker Service

The Go `EndpointPicker` interface only works for in-process Go implementations. To enable endpoint selection in **any language** (Rust, Python, C++, etc.), the Light EPP also defines a gRPC `EndpointPickerService` protobuf:

```protobuf
// pkg/epp-light/proto/picker.proto

syntax = "proto3";
package epplight;

service EndpointPickerService {
  rpc Pick(PickRequest) returns (PickResponse);
}

message PickRequest {
  map<string, string> headers = 1;
  bytes body = 2;
  string model = 3;
  repeated string candidate_subset = 4;
  repeated EndpointInfo endpoints = 5;
}

message EndpointInfo {
  string address = 1;
  string port = 2;
  string name = 3;
  map<string, string> labels = 4;
}

message PickResponse {
  string endpoint = 1;
  repeated string fallbacks = 2;
}
```

The protobuf messages mirror the Go types exactly (`RequestInfo` ↔ `PickRequest`, `Endpoint` ↔ `EndpointInfo`, `PickResult` ↔ `PickResponse`), making the mapping trivial.

#### How Both Paths Coexist

A `GRPCPicker` adapter implements the Go `EndpointPicker` interface by calling out to a remote gRPC service. The `server.go` Process loop doesn't change — it always calls `s.picker.Pick()`. The picker is either in-process or remote:

```
                       In-process (Go)
                      ┌────────────────────────┐
                      │  RandomPicker          │
                      │  or custom Go picker    │
                      └────────────────────────┘
                                ▲
                                │ (implements EndpointPicker)
                                │
server.go ──── picker.Pick() ──┤
                                │
                                │ (implements EndpointPicker via gRPC)
                                ▼
                      ┌────────────────────────┐
                      │  GRPCPicker            │──── gRPC ────► Remote Service
                      │  (adapter/client)       │               (Rust, Python, etc.)
                      └────────────────────────┘
```

The `--picker-address` CLI flag controls which path is used:
- **Unset** → in-process picker (default `RandomPicker`, or custom Go picker via `runner.WithPicker()`)
- **Set** (e.g., `--picker-address=localhost:9010`) → `GRPCPicker` connects to the remote service

#### Benefits of this Approach

1. **Language-agnostic** — any language with gRPC support can implement the picker service. The `.proto` file is the contract.
2. **No protocol reimplementation** — non-Go implementations don't need to understand ext-proc, Envoy metadata, subset filtering, or Kubernetes CRDs. The Go Light EPP handles all of that.
3. **Zero overhead for Go** — in-process Go pickers use a direct function call with no serialization or network hop.
4. **Independently deployable** — the picker service can be scaled, versioned, and deployed separately from the protocol layer.
5. **Testable** — the proto definition enables generating client/server stubs in any language for testing.

#### Implementing an EPP in Rust

A Rust implementation follows these steps:

**Step 1: Generate Rust stubs from `picker.proto`**

Add the proto to your Rust project and use `tonic-build`:

```rust
// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("path/to/picker.proto")?;
    Ok(())
}
```

**Step 2: Implement the `EndpointPickerService` trait**

```rust
use tonic::{Request, Response, Status};

pub struct MyRustPicker;

#[tonic::async_trait]
impl EndpointPickerService for MyRustPicker {
    async fn pick(
        &self,
        request: Request<PickRequest>,
    ) -> Result<Response<PickResponse>, Status> {
        let req = request.into_inner();

        // Custom selection logic using req.model, req.headers,
        // req.endpoints, req.candidate_subset, req.body, etc.
        let chosen = req.endpoints.first()
            .ok_or_else(|| Status::unavailable("no endpoints"))?;

        Ok(Response::new(PickResponse {
            endpoint: format!("{}:{}", chosen.address, chosen.port),
            fallbacks: vec![],
        }))
    }
}
```

**Step 3: Run the gRPC server**

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:9010".parse()?;
    let picker = MyRustPicker;

    Server::builder()
        .add_service(EndpointPickerServiceServer::new(picker))
        .serve(addr)
        .await?;

    Ok(())
}
```

**Step 4: Run the Go Light EPP pointing to the Rust service**

```bash
# Start the Rust picker service
cargo run  # listens on :9010

# Start the Go Light EPP, delegating selection to Rust
epp-light --pool-name=my-pool --pool-namespace=default --picker-address=localhost:9010
```

The full request flow becomes:

```
Client ──HTTP──► Envoy ──ext-proc──► Go Light EPP ──gRPC──► Rust Picker
                                     (protocol layer)        (selection logic)
                                     handles:                handles:
                                     - ext-proc state        - endpoint selection
                                     - metadata keys         - custom routing
                                     - subset filtering      - model affinity
                                     - pod discovery         - etc.
                                     - InferencePool CRD
```

The Rust implementation only needs to decide *which* endpoint — everything else is handled by the Go protocol layer.

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
    picker_grpc.go         — GRPCPicker: implements EndpointPicker via remote gRPC service
    metadata.go            — EPP protocol constants (proposal 004)
    datastore.go           — Simplified datastore (pool + pods only)
    server.go              — Ext-proc StreamingServer with Process loop
    request.go             — Request handling, metadata generation, subset filtering
    response.go            — Response handling
    proto/
        picker.proto       — EndpointPickerService protobuf definition
        gen/               — Generated Go stubs (picker.pb.go, picker_grpc.pb.go)
    controller/
        pool.go            — InferencePool v1 reconciler
        pod.go             — Pod reconciler
    server/
        runner.go          — ExtProcServerRunner (gRPC wiring)
        options.go         — Minimal CLI flags (including --picker-address)
cmd/epp-light/
    main.go                — Entrypoint
    runner/
        runner.go          — Runner with WithPicker() and gRPC picker wiring
```

17 files total, versus dozens in `pkg/epp/`.

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

### Usage Examples

#### Option A: Custom Go Picker (in-process)

```go
package main

import (
    "os"

    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/gateway-api-inference-extension/cmd/epp-light/runner"
    epplight "sigs.k8s.io/gateway-api-inference-extension/pkg/epp-light"
)

type ModelAwarePicker struct{}

func (p *ModelAwarePicker) Pick(
    ctx context.Context,
    req *epplight.RequestInfo,
    endpoints []epplight.Endpoint,
) (*epplight.PickResult, error) {
    for _, ep := range endpoints {
        if ep.Labels["model"] == req.Model {
            return &epplight.PickResult{Endpoint: ep.Address + ":" + ep.Port}, nil
        }
    }
    if len(endpoints) > 0 {
        ep := endpoints[0]
        return &epplight.PickResult{Endpoint: ep.Address + ":" + ep.Port}, nil
    }
    return nil, fmt.Errorf("no endpoints for model %q", req.Model)
}

func main() {
    if err := runner.NewRunner().WithPicker(&ModelAwarePicker{}).Run(ctrl.SetupSignalHandler()); err != nil {
        os.Exit(1)
    }
}
```

#### Option B: Remote Picker via gRPC (any language)

No custom Go code needed — just run the binary with `--picker-address`:

```bash
epp-light \
    --pool-name=my-pool \
    --pool-namespace=default \
    --picker-address=localhost:9010
```

The remote service at `localhost:9010` implements `EndpointPickerService` from `picker.proto` in any language. See [Implementing an EPP in Rust](#implementing-an-epp-in-rust) for a complete example.

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

### 1. Go interface only, no gRPC picker service

A Rust or Python EPP would have to reimplement the entire ext-proc protocol layer (Process loop, state machine, metadata generation, subset filtering, pod discovery). This is hundreds of lines of protocol-specific code that has nothing to do with endpoint selection. The gRPC picker service lets non-Go implementations focus purely on selection logic while the Go Light EPP handles everything else.

### 2. gRPC only, no in-process Go interface

All pickers (including Go ones) would communicate via gRPC. This simplifies the architecture (one path instead of two) but adds a network hop and serialization overhead for Go pickers that don't need it. The dual-path approach (in-process for Go, gRPC for others) gives zero overhead when it's not needed.

### 3. Define EndpointPicker as a plugin within the existing framework

The existing plugin framework (`framework/interface/scheduling/plugins.go`) defines Filter, Scorer, and Picker as separate interfaces composed into SchedulerProfiles. This is powerful but forces implementors to understand the profile/filter/scorer/picker pipeline. A single `Pick` method is a lower abstraction barrier.

### 4. Use `fwkdl.Endpoint` interface instead of a flat struct

The existing `fwkdl.Endpoint` interface requires `GetMetrics()`, `GetAttributes()`, `UpdateMetrics()`, and the `EndpointFactory` abstraction. This pulls in the data layer framework. A simple struct with Address, Port, Name, Labels is sufficient for routing decisions and avoids the coupling. The flat struct also maps cleanly to the `EndpointInfo` protobuf message for cross-language support.

## Testing

### Unit Tests

- `picker_random_test.go` — Random selection, empty list error, distribution across endpoints
- `picker_grpc_test.go` — GRPCPicker with mock gRPC server: verify Go→proto→Go round-trip for all types
- `datastore_test.go` — Pool set/get, pod CRUD, label matching, endpoint listing, active ports
- `server_test.go` — Ext-proc Process loop with mock stream (header-only, body, subset filtering, no-endpoints)
- `request_test.go` — `extractModelFromBody`, `extractCandidateSubset`, `filterEndpointsBySubset`, metadata generation
- `controller/*_test.go` — Pool and pod reconcilers with fake k8s client

### Integration Tests

- Start full Light EPP against a fake k8s client with in-process picker, send ext-proc requests via gRPC client, verify endpoint selection in headers and dynamic metadata
- Start full Light EPP with `--picker-address` pointing to a test gRPC picker server, verify the remote picker is called and endpoints are returned correctly

### Conformance Checks

Per proposal 004:

- `x-gateway-destination-endpoint` header set on every successful response
- `envoy.lb` dynamic metadata set with matching endpoint value
- Subset filtering respects `envoy.lb.subset_hint`
- 503 returned when no endpoints available
- 503 returned when candidate subset matches no available endpoints

### Build Verification

```bash
make generate-proto-light                          # Generate proto code
go build ./pkg/epp-light/... ./cmd/epp-light/...   # Build all packages
go vet ./pkg/epp-light/... ./cmd/epp-light/...     # Vet
```

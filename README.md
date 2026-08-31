# Governance-as-Code (GaC) — Master Reference

> **Enterprise Data Platform · Governance-as-Code + AI Governance**
> Version 2.0.0 · Classification: Principal Architecture Document

---

## Table of Contents

| Layer | Audience | Section |
|---|---|---|
| 🏢 **Business Overview** | Executives, Product Managers | [§1 — Why This System Exists](#1-why-this-system-exists) |
| 🗺️ **Architecture Overview** | Architects, Tech Leads | [§2 — Four-Plane Topology](#2-four-plane-topology) |
| 🔄 **End-to-End Flow** | Architects, Senior Engineers | [§3 — End-to-End Flow Summary](#3-end-to-end-flow-summary) |
| 🧩 **Design Patterns** | Engineers, Architects | [§4 — Design Patterns Catalogue](#4-design-patterns-catalogue) |
| 📦 **Module Reference** | Engineers | [§5 — Module-by-Module Code Reference](#5-module-by-module-code-reference) |
| 🔌 **Contracts & Types** | Engineers | [§6 — Shared Contract Layer](#6-shared-contract-layer) |
| ⚠️ **Failure Taxonomy** | Engineers, SREs | [§7 — Failure Taxonomy](#7-failure-taxonomy) |
| 📡 **Event Taxonomy** | Engineers, SREs | [§8 — Event Taxonomy](#8-event-taxonomy) |
| 🗄️ **Data Stores** | Architects, DBAs | [§9 — Data Stores and Protocols](#9-data-stores-and-protocols) |
| 🚀 **Developer Quickstart** | Engineers | [§10 — Developer Quickstart](#10-developer-quickstart) |
| 📁 **Repository Map** | Engineers | [§11 — Repository Map](#11-repository-map) |

---

## 1. Why This System Exists

### The Problem

In a modern data platform, **governance is an afterthought applied manually**:

- A data engineer writes a Spark job that reads PII data — nobody knows until audit.
- A security admin manually configures Ranger ACLs — out of sync within days.
- A model scientist changes model hyperparameters — no audit trail, no compliance gate.
- A production cluster diverges from what was agreed in design — discovered during a breach.

### The Solution: Governance as Code

**GaC (Governance-as-Code)** treats data access policies, metadata schemas, lineage graphs, and AI model governance contracts exactly like application code:

- **Version-controlled** in Git — every policy change is a commit, every change is reviewable.
- **Shift-Left** — governance constraints are evaluated at the developer's workstation, before any code reaches production.
- **Deterministic** — the same code always produces the same policies. No manual steps.
- **Self-healing** — a background operator continuously detects and repairs drift between what Git says should be true and what the cluster is actually running.

### Business Value

| Capability | Business Impact |
|---|---|
| Shift-Left governance validation | Catch PII violations at PR time, not incident time |
| GitOps policy lifecycle | Full audit trail of every ACL change |
| Automated drift detection | SLA compliance — cluster never diverges from policy for more than N seconds |
| AI Model Governance | Regulatory compliance for Private AI / Agentic AI workloads |
| Discovery Engine-based data discovery | Automated lineage coverage for systems without native hooks |
| Extended ADT response model | Zero ambiguity in governance decisions — 4 typed outcomes, no nulls |

---

## 2. Four-Plane Topology

The system is decomposed into four orthogonal planes. Each plane has a single responsibility, communicates with others via well-defined protocols, and applies a named architectural pattern.

```
+-----------------------------------------------------------------------+
|  PLANE 1 · Shift-Left Developer Proxy                                 |
|  Pattern: CQRS (Query-only) + Extended Algebraic Data Types (ADTs)    |
|  Who: Developer / Data Scientist at their workstation                 |
|  What: Dry-run governance checks before code reaches Git              |
+------------------------------+----------------------------------------+
                               | git push / PR
+------------------------------v----------------------------------------+
|  PLANE 2 · GaC State Compiler & CI/CD Engine                          |
|  Pattern: Hexagonal Architecture + Explicit Finite State Machine      |
|  Who: CI/CD Runner (automated)                                        |
|  What: Compile code diffs -> Ranger policies + Atlas TypeDefs + AI gov|
+-------------+-----------------------------+----------------------------+
              | Kafka / Atlas REST          | REST PUT (outbound ports)
+-------------v------------+   +-----------v---------------------------+
|  PLANE 3 · Federated     |   |  PLANE 4 · Governance Reconciliation Operator|
|  Execution & Telemetry   |   |  Pattern: Control Loop / Drift Detect |
|  Pattern: Event-Driven   |   |  Who: Kubernetes Operator (background)|
|  Pub/Sub (Kafka)         |   |  What: Detect & auto-remediate drift  |
+--------------------------+   +---------------------------------------+
```

### Shared Data Stores (Cross-Plane)

| Store | Role | Accessed By |
|---|---|---|
| **Git Repository** | Source of Truth — desired state for all policies | Planes 1, 2, 4 |
| **Apache Ranger** | ACL Policy Engine — access control enforcement | Planes 1, 2, 4 |
| **Apache Atlas** | Metadata Catalog + Lineage Store | Planes 2, 3, 4 |
| **Model Registry** | AI Model Governance — weights, hyperparams, transparency contracts | Planes 1, 2, 4 |

---

## 3. End-to-End Flow Summary

> For the full sequence diagram, see [`docs/architecture-micro-v2.md`](docs/architecture-micro-v2.md).

### Phase Overview

```
Phase 1 — Local Dry-Run (Plane 1)
──────────────────────────────────
Developer submits Spark/Trino code OR model change diff to LocalProxy.
LocalProxy performs CQRS reads against Trino, Ranger, and Model Registry.
Returns one of 4 ADT variants:
  +------------------------------------------------------------------+
  |  Allow                 -> proceed to git push                    |
  |  Deny(reason)          -> fix locally, retry dry-run             |
  |  Mask(columns)         -> adjust pipeline for masked schema      |
  |  ModelRestrict(constr) -> revise model change, retry             |
  +------------------------------------------------------------------+

Phase 2 — Git Push & Webhook (Planes 1 -> 2)
──────────────────────────────────────────────
Developer opens PR. GitHub Webhook fires -> CI/CD Runner.
Payload carries change_type: DATA_PIPELINE | MODEL_CHANGE | BOTH

Phase 3 — Core Domain Compilation (Plane 2)
─────────────────────────────────────────────
State Machine: WEBHOOK_RECEIVED -> Code_Analyzed
Core Domain: inferLineage() + inferDataPolicies()
             inferModelGovernance() + inferAITransparencyContracts()
State Machine: INFERENCE_COMPLETE -> Policy_Compiled
CI Dry-Run via LocalProxy (same CQRS path as Phase 1).
State Machine: DRY_RUN_PASSED -> Dry_Run_Passed

Phase 4 — Outbound Port Provisioning (Plane 2)
────────────────────────────────────────────────
Dual outbound ports fire in parallel:
  PolicyCompiler    -> Ranger REST PUT /policies
                    -> Atlas  REST PUT /typedefs
  ModelGovCompiler  -> Model Registry  REST PUT /models/{id}/governance
                    -> Model Registry  REST PUT /models/{id}/transparency
State Machine: PROVISIONING_COMPLETE -> Policy_Enforced
GitHub PR: APPROVED + merge()

Phase 5 — Federated Execution (Plane 3)
─────────────────────────────────────────
Developer submits merged job to Spark / Trino.
Native Atlas Hooks emit fire-and-forget to Kafka ATLAS_HOOK topic.
DiscoveryEngine Engine runs autonomously:
  -> Kafka ATLAS_HOOK (async path)
  -> Atlas REST POST /entities (direct batch path for high-volume)

Phase 6 — Reconciliation Loop (Plane 4, continuous)
─────────────────────────────────────────────────────
Every N seconds, ReconOperator:
  1. Fetches desired state from Git
  2. Fetches actual state from Ranger + Atlas + Model Registry REST APIs
  3. Three-dimensional diff: Ranger ACL | Atlas TypeDef | model registry governance
  4. On drift -> DriftDetectedEvent -> auto-remediation -> RemediationDoneEvent
```

### State Machine (Plane 2)

```
[Code_Analyzed] --INFERENCE_COMPLETE--> [Policy_Compiled]
                                               |
                                        DRY_RUN_PASSED
                                  (BOTH data + model dry-runs must pass)
                                               |
                                        [Dry_Run_Passed]
                                               |
                                    PROVISIONING_COMPLETE
                                  (BOTH compilers must succeed)
                                               |
                                        [Policy_Enforced]

Any invalid event in any state -> REJECTED (pipeline blocked, PR not merged)
```

### Async Boundaries

Three explicit fire-and-forget boundaries — **compute threads are never blocked**:

| Boundary | Producer | Consumer | Mechanism |
|---|---|---|---|
| Spark/Trino → Kafka | Native Atlas Hook | Kafka Broker | Kafka producer |
| Kafka → Atlas | Kafka Broker | Atlas ingest consumer | Kafka consumer group |
| DiscoveryEngine → Kafka | discovery scan job | Kafka Broker + Atlas | Dual: Kafka async + Atlas REST batch |

---

## 4. Design Patterns Catalogue

| Pattern | Where Applied | Why |
|---|---|---|
| **CQRS (Query side only)** | Plane 1 — LocalProxy | Separates read path from write path. Proxy only queries; never mutates governance state. |
| **Extended Algebraic Data Types (ADTs)** | Plane 1 — dry-run result | Total, exhaustive, unambiguous response model. 4 typed variants, no nulls, no booleans. |
| **Hexagonal Architecture** | Plane 2 — Core Domain | Core domain logic has zero I/O. GitHub Webhook is an Inbound Port. Ranger/Atlas/Model Registry are Outbound Ports. |
| **Explicit Finite State Machine** | Plane 2 — PR lifecycle | PR lifecycle is a deterministic automaton. Invalid state transitions are rejected. |
| **Hexagonal Outbound Ports** | Plane 2 — PolicyCompiler + ModelGovCompiler | Core Domain never calls HTTP directly. Two independent port classes handle serialisation. |
| **Event-Driven Architecture (Pub/Sub)** | Plane 3 — Telemetry Bus | Fire-and-forget telemetry. Producers and Atlas consumer are temporally decoupled. |
| **Control Loop / Declarative State Matching** | Plane 4 — ReconOperator | Kubernetes-style reconciler. Drift triggers auto-remediation. |
| **Result[S, F] ADT (Railway-Oriented Programming)** | Cross-cutting — all planes | Errors are values, never exceptions at domain boundaries. |
| **Sealed Type Hierarchies** | Contracts layer | GovernanceFailure, GovernanceEvent, Result are sealed — unauthorised subtypes rejected at class definition time. |
| **Value Objects (frozen dataclasses)** | Contracts layer | All domain objects are immutable. `frozen=True, slots=True`. Zero mutation surface. |

---

## 5. Module-by-Module Code Reference

### Plane 1 — `src/local_proxy/`

#### [`trino_plan_stub.py`](src/local_proxy/trino_plan_stub.py)

**Responsibility:** Produces a deterministic fake Trino execution plan (`EXPLAIN (FORMAT JSON)` shape) from a raw SQL string. Enables offline dry-run validation without a live Trino cluster.

**Key function:**
```python
def generate_trino_execution_plan(sql: str) -> dict[str, Any]
```

**Detection rules:**
- Table name matched against `_PII_TABLES` frozenset (in production: Atlas CQRS read).
- `SELECT *` flagged — `MASKING_REQUIRED` ADT variant may be returned.
- Every plan node gets a **deterministic UUID** derived from `sha256(sql)` — idempotent dry-run.
- Returns a `TableScan` tree mirroring Trino's internal `PlanNode` JSON.

---

### Plane 2 — `src/gac_compiler/`

#### [`core_domain.py`](src/gac_compiler/core_domain.py)

**Responsibility:** The Hexagonal Core Domain. Accepts a Trino execution plan dict and compiles it into governance objects. **Zero I/O. Zero network calls.**

**Key function:**
```python
def compile_plan(
    plan: dict[str, Any],
    *,
    git_sha: str = "unknown",
    requesting_principal: str = "ci-cd-runner",
) -> Result[CompilationResult, GovernanceFailure]
```

**Compilation pipeline — 3 passes over TableScan nodes:**

| Pass | Output | Logic |
|---|---|---|
| 1 | `RangerPolicy` objects | Non-PII → ALLOW. PII + SELECT* → DENY. PII columns → MASKING policy per column. |
| 2 | `AtlasTypeDef` objects | One TypeDef per unique table. Columns → `AtlasAttributeDef`. Sub-typed as `DataSet`. |
| 3 | `AtlasEntity` objects | One entity per table. PII tables tagged `AtlasClassification("PII", propagate=ENABLED)`. |

**Policy matrix (Ranger inference):**

```
Non-PII table        -> ALLOW read policy for CI principal
PII + SELECT *       -> DENY SELECT * + MASKING policy per PII column
PII + explicit cols  -> ALLOW on non-PII + MASKING policy per PII column
```

**Deterministic GUID:** `uuid.UUID(bytes=sha256(qualified_name).digest()[:16])` — same table always gets the same GUID across all runs.

---

#### [`outbound_port.py`](src/gac_compiler/outbound_port.py)

**Responsibility:** The Hexagonal Outbound Port. Serialises `CompilationResult` (from Core Domain) into JSON artefact files on disk.

**File layout produced:**
```
outputs/
  ranger_policies/<policy_name>.json    <- one file per RangerPolicy
  atlas_typedefs/<type_name>.json       <- one file per AtlasTypeDef
  atlas_entities/<qualified_name>.json  <- one file per AtlasEntity
  manifest.json                         <- index; presence = complete atomic run
```

Every write returns `Result[Path, GovernanceFailure]` — never raises. Manifest written last signals atomic completion.

---

### Plane 3 — `src/telemetry_bus/`

#### [`event_bus.py`](src/telemetry_bus/event_bus.py)

**Responsibility:** In-memory async Pub/Sub event bus. Simulates Apache Kafka's `ATLAS_HOOK` and governance topics using Python's `asyncio.Queue`.

**Kafka analogy mapping:**

| Kafka Concept | EventBus Equivalent |
|---|---|
| Kafka Broker | `EventBus` instance (singleton per runtime) |
| Kafka Topic | Per-topic `asyncio.Queue` inside the bus |
| `Producer.send()` | `EventBus.publish()` |
| `Consumer.poll()` | `EventBus.subscribe()` async generator |
| Consumer Group | Named `subscriber_id` per topic |
| Kafka Offset | `GovernanceEvent.sequence` (monotonic int) |

**Key API:**
```python
# Publisher -- fire-and-forget (any plane)
seq: int = await bus.publish(event)

# Subscriber -- async generator (Plane 4)
async for event in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="recon"):
    await handle(event)
```

**Fan-out semantics:** Every subscriber with a different `subscriber_id` receives all events independently — mirrors separate Kafka consumer groups.

---

#### [`events.py`](src/telemetry_bus/events.py)

Defines all typed event value objects. See [§8 — Event Taxonomy](#8-event-taxonomy).

---

### Plane 4 — `src/recon_operator/`

#### [`control_loop.py`](src/recon_operator/control_loop.py)

**Responsibility:** Async control loop implementing the Reconciliation Operator. Runs every `tick_interval_s` seconds.

**Tick lifecycle:**
```
1. _load_desired_state()   -> reads JSON files from gac_compiler outputs/
2. store.actual_state()    -> frozen snapshot from telemetry events
3. diff(desired, actual)   -> per governance dimension (ranger, atlas)
4. On drift:
     _publish_drift()      -> DriftDetectedEvent to bus
     _remediate()          -> stub REST PUT + RemediationDoneEvent
```

**Diagnostics:**
```python
loop.stats()  # -> {"ticks": int, "total_drifts": int, "remediations": int}
```

---

#### [`differ.py`](src/recon_operator/differ.py)

**Responsibility:** 100% pure recursive dictionary diff engine. No I/O, no async, no side effects.

**Diff kinds:**

| `DiffKind` | Meaning |
|---|---|
| `ADDED` | Key present in desired but absent in actual |
| `REMOVED` | Key present in actual but absent in desired |
| `MODIFIED` | Key present in both, values differ |
| `TYPE_MISMATCH` | Same key, incompatible types (e.g., dict vs str) |

**Severity scoring:** Inferred from key criticality (`isEnabled`, `git_sha` -> HIGH).

---

#### [`state_store.py`](src/recon_operator/state_store.py)

Accumulates actual state from telemetry bus events. Returns a frozen snapshot for the differ — coroutine-safe.

---

### UI — `src/ui/`

#### [`app.py`](src/ui/app.py)

Streamlit dashboard with three tabs:
- **Dashboard & Diffs** — Drift reconciliation report with colour-coded diff table (green=ADDED, red=REMOVED, amber=MODIFIED) plus verbose terminal output.
- **Event Flow** — Step-by-step event replay with JSON inspector and progress bar.
- **Playground** — Emit raw `LineageEvent` objects to the live EventBus; trigger full E2E simulation.

#### [`facade.py`](src/ui/facade.py)

Anti-corruption layer between Streamlit UI and the domain. Manages daemon lifecycle, event accumulation, and view-model projection.

#### [`daemon.py`](src/ui/daemon.py)

Background thread wrapper that runs the `ControlLoop` and `EventBus` in an isolated `asyncio` event loop, decoupled from Streamlit's main thread.

---

## 6. Shared Contract Layer

**Location:** [`src/contracts/`](src/contracts/)

The contract layer defines **all shared value objects** and is the **only cross-module import boundary**. Every other module imports exclusively from `contracts` — never from sibling modules.

```python
from contracts import (
    Result, Success, Failure, ok, err,
    RangerPolicy, AtlasEntity, AtlasTypeDef, AtlasLineage,
    GovernanceFailure, ValidationFailure, PolicyFailure,
    MetadataFailure, ReconciliationFailure, TransportFailure,
)
```

### Result ADT — [`result.py`](src/contracts/result.py)

Sealed sum type. Only `Success` and `Failure` are permitted subtypes — enforced via `__init_subclass__`.

```python
Result[S, F]
  +-- Success(value: S)
  +-- Failure(error: F)

# Combinators
result.fold(on_success, on_failure)   # exhaustive eliminator -- MUST handle both branches
result.map(f)                          # transform success value, propagate failure unchanged
result.flat_map(f)                     # monadic bind -- chain fallible operations
result.recover(f)                      # extract with recovery fallback
result.get_or_raise()                  # unsafe extractor -- use only at CLI boundaries
```

### Extended Dry-Run ADT (Plane 1)

```
Result<Allow | Deny<Reason> | Mask<Columns> | ModelRestrict<Constraint>>

  Allow
    -> Principal authorised. No restrictions. Proceed to git push.

  Deny<Reason>
    -> Access rejected.
    -> Reason: {rule_id, policy_name, violated_clause}

  Mask<Columns>
    -> Access granted but pipeline must operate on masked schema.
    -> Columns: [{col_name, mask_type, pii_class}]
    -> Applicable for PII / Private AI scenarios.

  ModelRestrict<Constraint>
    -> Model change violates governance boundary.
    -> Constraint: {constraint_id, violated_contract,
                    ai_transparency_clause, agentic_ai_risk_class}
    -> Developer must revise model diff before re-submission.
```

### RangerPolicy — [`ranger_policy.py`](src/contracts/ranger_policy.py)

| Type | Role |
|---|---|
| `RangerPolicy` | Top-level policy container (name, service, resources, policy items, git_sha) |
| `RangerPolicyItem` | Single ACL rule — principal + accesses + effect + optional masking |
| `RangerPrincipal` | Identity container — users / groups / roles tuple |
| `RangerResource` | Addressed resource — TABLE or COLUMN with values |
| `RangerMaskingSpec` | Column masking specification — `MaskType` enum |
| `PolicyEffect` | `ALLOW` or `DENY` |
| `MaskType` | `HASH` / `MASK_LAST_4` / `NULLIFY` / `REDACT` / `PARTIAL_MASK` / `CUSTOM` |

### Atlas Types — [`atlas_entity.py`](src/contracts/atlas_entity.py)

| Type | Role |
|---|---|
| `AtlasTypeDef` | Type system declaration — sub-typed from `DataSet` |
| `AtlasAttributeDef` | Column-level attribute in a TypeDef |
| `AtlasEntity` | Dataset entity instance — aggregate root |
| `AtlasClassification` | PII / sensitivity tag — with propagation control |
| `AtlasLineage` | Directed lineage edge — source → target via process |
| `AtlasLineageEdge` | Single edge in a lineage graph |
| `EntityStatus` | `ACTIVE` or `DELETED` |
| `ClassificationPropagation` | `ENABLED` / `DISABLED` / `TO_PROPAGATED_ENTITIES` |
| `LineageDirection` | `INPUT` / `OUTPUT` / `BOTH` |
| `CardinalityType` | `SINGLE` / `LIST` / `SET` |

---

## 7. Failure Taxonomy

All failures are **immutable value objects**, not exceptions. `GovernanceFailure` is sealed — only 5 permitted subtypes.

```
GovernanceFailure (sealed base)
  +-- ValidationFailure      -> Plane 1  (LocalProxy dry-run failures)
  +-- PolicyFailure          -> Plane 2  (PolicyCompiler / ModelGovCompiler)
  +-- MetadataFailure        -> P2 / P3  (Atlas registration, lineage ingest, DiscoveryEngine)
  +-- ReconciliationFailure  -> Plane 4  (drift detection + remediation)
  +-- TransportFailure       -> Any plane (network / HTTP / Kafka transport)
```

### `ValidationFailure` (Plane 1)

| `ValidationCode` | Meaning |
|---|---|
| `SCHEMA_NOT_FOUND` | Table or schema does not exist in Trino catalog |
| `COLUMN_NOT_FOUND` | Referenced column missing in schema |
| `ACCESS_DENIED` | Ranger policy rejects the principal |
| `MASKING_REQUIRED` | PII column accessed without masking — pipeline must be adjusted |
| `MODEL_RESTRICT` | Model change violates Model Registry governance constraint |
| `INVALID_QUERY_SYNTAX` | SQL failed Trino parse phase |
| `CLASSIFICATION_MISSING` | Required PII classification tag absent from Atlas |

### `PolicyFailure` (Plane 2)

| `PolicyFailureCode` | Meaning |
|---|---|
| `RANGER_API_ERROR` | Ranger REST returned non-2xx |
| `RANGER_CONFLICT` | Policy name collision in Ranger |
| `RANGER_VALIDATION_ERROR` | Ranger rejected policy payload |
| `MODEL_REGISTRY_API_ERROR` | Model Registry REST returned non-2xx |
| `MODEL_CONTRACT_CONFLICT` | AI transparency contract hash conflict |
| `COMPILATION_ERROR` | Core Domain produced zero policies from plan |
| `STATE_MACHINE_VIOLATION` | Invalid PR lifecycle state transition attempted |

### `MetadataFailure` (Planes 2/3)

| `MetadataFailureCode` | Meaning |
|---|---|
| `ATLAS_API_ERROR` | Atlas REST returned non-2xx |
| `TYPEDEF_CONFLICT` | TypeDef exists with incompatible schema |
| `ENTITY_NOT_FOUND` | Referenced entity GUID missing from Atlas |
| `LINEAGE_CYCLE_DETECTED` | Lineage graph would become cyclic |
| `GUID_COLLISION` | Deterministic GUID collision (sha256 truncation edge case) |
| `KAFKA_PUBLISH_ERROR` | Telemetry bus write failure |
| `DISCOVERY_SCAN_ERROR` | Data Discovery Engine scan failure |

### `ReconciliationFailure` (Plane 4)

| `ReconciliationFailureCode` | Meaning |
|---|---|
| `DESIRED_STATE_FETCH_ERROR` | Git API unreachable |
| `ACTUAL_STATE_FETCH_ERROR` | Ranger / Atlas / Model Registry API unreachable |
| `REMEDIATION_FAILED` | Patch attempt rejected by target system |
| `DIFF_PARSE_ERROR` | State comparison produced invalid output |
| `MAX_RETRIES_EXCEEDED` | Remediation exhausted retry budget |

> **Invariant:** `ReconciliationFailure.dimension` must be one of `{"ranger", "atlas", "model-registry", "unknown"}` — enforced in `__post_init__`.

### `TransportFailure` (Cross-cutting)

| `TransportFailureCode` | Meaning |
|---|---|
| `CONNECTION_TIMEOUT` | TCP connect timed out |
| `CONNECTION_REFUSED` | Port closed / service down |
| `TLS_ERROR` | Certificate validation failed |
| `HTTP_5XX` | HTTP server error |
| `HTTP_4XX` | HTTP client error |
| `KAFKA_BROKER_UNAVAILABLE` | No reachable Kafka broker |
| `DNS_RESOLUTION_FAILED` | Hostname not resolvable |

---

## 8. Event Taxonomy

All events are **frozen dataclasses** (immutable). `GovernanceEvent` is the sealed base — only 5 permitted subtypes.

Every event carries a standard envelope:

| Field | Type | Description |
|---|---|---|
| `event_id` | `str` (UUID) | Unique event identifier — set at construction |
| `emitted_at` | `datetime` (UTC) | Emission timestamp |
| `sequence` | `int` | Monotonic offset assigned by bus on `publish()` — Kafka offset equivalent |

### Topic Map

| `Topic` | Kafka Equivalent | Published By | Consumed By |
|---|---|---|---|
| `ATLAS_HOOK` | `ATLAS_HOOK` | Spark hook, Trino hook, DiscoveryEngine | Atlas (async) |
| `POLICY_COMPILED` | `policy.compiled` | Plane 2 — gac_compiler | Plane 4 — recon_operator |
| `ENTITY_REGISTERED` | `atlas.entity.registered` | Plane 3 (Atlas ingest confirmation) | Plane 4 |
| `DRIFT_DETECTED` | `governance.drift` | Plane 4 — recon_operator | Observers / alerting |
| `REMEDIATION_DONE` | `governance.remediation` | Plane 4 — recon_operator | Observers / audit log |

### `LineageEvent` — topic: `ATLAS_HOOK`

```python
LineageEvent(
    source_table: str,               # "staging.raw_data"
    target_table: str,               # "prod.cleaned_data"
    process_name: str,               # Spark job name or Trino session
    source_system: str,              # "spark-hook" | "trino-hook" | "discovery-engine"
    job_id: str,                     # Job/session/scan identifier
    classification_tags: tuple[str, ...],  # ("PII", "Confidential")
)
```

### `PolicyCompiledEvent` — topic: `POLICY_COMPILED`

```python
PolicyCompiledEvent(
    plan_id: str,                    # Deterministic query UUID
    git_sha: str,                    # Triggering commit SHA
    policy_names: tuple[str, ...],   # All compiled RangerPolicy names
    typedef_names: tuple[str, ...],  # All compiled AtlasTypeDef names
    entity_names: tuple[str, ...],   # All compiled AtlasEntity qualified names
    output_dir: str,                 # Path to gac_compiler outputs/
)
```

### `DriftDetectedEvent` — topic: `DRIFT_DETECTED`

```python
DriftDetectedEvent(
    dimension: str,       # "ranger" | "atlas" | "model-registry"
    resource_id: str,     # Policy name or entity qualified_name
    desired_hash: str,    # Hash of desired-state payload
    actual_hash: str,     # Hash of actual-state payload
    severity: str,        # "LOW" | "MEDIUM" | "HIGH"
    verbose_report: str,  # Full DiffResult.report() output
)
```

### `RemediationDoneEvent` — topic: `REMEDIATION_DONE`

```python
RemediationDoneEvent(
    dimension: str,         # "ranger" | "atlas" | "model-registry"
    resource_id: str,       # The resource that was patched
    drift_event_id: str,    # event_id of the triggering DriftDetectedEvent
    patch_applied: str,     # Human-readable patch description
)
```

---

## 9. Data Stores and Protocols

| Store | Write Protocol | Read Protocol | Written By | Read By |
|---|---|---|---|---|
| **Git** | `git push` / PR merge | REST `GET /contents` | Developer, CI/CD runner | Plane 4 ReconOperator |
| **Apache Ranger** | REST `PUT /policies` | REST `GET /policies` | Plane 2 PolicyCompiler, Plane 4 Remediation | Plane 1 LocalProxy, Plane 4 Differ |
| **Apache Atlas** | REST `PUT /typedefs`, `POST /entities` | REST `GET /entities` | Plane 2 PolicyCompiler, Plane 3 Kafka consumer, DiscoveryEngine | Plane 4 Differ |
| **Model Registry** | REST `PUT /models/{id}/governance`, `PUT /models/{id}/transparency` | REST `GET /models` | Plane 2 ModelGovCompiler, Plane 4 Remediation | Plane 1 LocalProxy, Plane 4 Differ |
| **Kafka (ATLAS_HOOK)** | `Producer.send()` (async) | `Consumer.poll()` (async) | Spark hook, Trino hook, DiscoveryEngine | Atlas ingest consumer |
| **Kafka (governance topics)** | `EventBus.publish()` | `EventBus.subscribe()` | Planes 2, 3, 4 | Plane 4 StateStore, observers |

---

## 10. Developer Quickstart

### Prerequisites

- Python 3.11+
- (Optional) `uvloop` for accelerated asyncio

### Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install streamlit pandas
```

### Run the E2E Simulation

```bash
python run_simulation.py
```

Executes the full 6-phase pipeline in memory:

1. Generates a Trino execution plan from a SQL string (Plane 1 stub)
2. Compiles it through the Core Domain (Plane 2)
3. Writes governance artefacts to `/tmp/gac_outputs/` (Outbound Port)
4. Publishes `PolicyCompiledEvent` and `LineageEvent` objects to the EventBus (Planes 2/3)
5. Runs the ControlLoop for N ticks, detecting drift (Plane 4)
6. Publishes `DriftDetectedEvent` + `RemediationDoneEvent` objects

### Run the Streamlit UI

```bash
python start_streamlit.py
# Navigate to http://localhost:8501
# Stop with:
python stop_streamlit.py
```

### Run Module Smoke Tests

```bash
python -m local_proxy._smoke_test
python -m gac_compiler._pipeline_smoke_test
python -m telemetry_bus._demo
python -m recon_operator._demo
```

---

## 11. Repository Map

```
governance/
|
+-- README.md                               <- This document
|
+-- docs/
|   +-- architecture-macro-v2.md           <- Macro architecture (4-plane + Mermaid graph)
|   +-- architecture-micro-v2.md           <- Micro architecture (full sequence diagram)
|
+-- src/
|   +-- contracts/                     <- SHARED CONTRACT LAYER (single import boundary)
|   |   +-- __init__.py                <- Public API -- import only from here
|   |   +-- result.py                  <- Result[S,F] ADT (sealed Success | Failure)
|   |   +-- failures.py                <- GovernanceFailure hierarchy (5 sealed subtypes)
|   |   +-- ranger_policy.py           <- RangerPolicy + masking types
|   |   +-- atlas_entity.py            <- AtlasEntity, AtlasTypeDef, AtlasLineage
|   |
|   +-- local_proxy/                   <- PLANE 1: Shift-Left Developer Proxy
|   |   +-- trino_plan_stub.py         <- Trino EXPLAIN stub (CQRS query side)
|   |   +-- _smoke_test.py
|   |
|   +-- gac_compiler/                  <- PLANE 2: GaC State Compiler & CI/CD Engine
|   |   +-- core_domain.py             <- Hexagonal Core Domain (pure, zero I/O)
|   |   +-- outbound_port.py           <- Hexagonal Outbound Port (JSON serialisation)
|   |   +-- outputs/                   <- Compiled governance artefacts (JSON files)
|   |   +-- _pipeline_smoke_test.py
|   |
|   +-- telemetry_bus/                 <- PLANE 3: Federated Execution & Telemetry Bus
|   |   +-- event_bus.py               <- In-memory asyncio Kafka simulation
|   |   +-- events.py                  <- Typed event value objects (5 sealed variants)
|   |   +-- _demo.py
|   |
|   +-- recon_operator/                <- PLANE 4: Governance Reconciliation Operator
|   |   +-- control_loop.py            <- Async control loop (drift detect + remediation)
|   |   +-- differ.py                  <- Pure recursive dict diff engine
|   |   +-- state_store.py             <- Telemetry-driven actual state accumulator
|   |   +-- _demo.py
|   |
|   +-- ui/                            <- STREAMLIT DASHBOARD
|       +-- app.py                     <- Streamlit multi-tab UI
|       +-- facade.py                  <- Anti-corruption layer (UI -> domain)
|       +-- daemon.py                  <- Background thread (asyncio event loop isolation)
|
+-- run_simulation.py                  <- E2E integration simulation script
+-- start_streamlit.py                 <- Launch Streamlit dashboard
+-- stop_streamlit.py                  <- Graceful shutdown
```

---

## Design Principles

| Principle | Implementation |
|---|---|
| **Determinism** | All governance state changes driven by versioned Git commits (GitOps). Same commit always produces same policies. |
| **Decoupling** | 4 planes communicate only through well-defined protocols (REST, Kafka, Webhook). No direct function calls across plane boundaries. |
| **Shift-Left** | Governance constraints — including model security — evaluated at developer workstation before any commit. |
| **Drift Detection** | Reconciliation Operator enforces desired state across data policies, metadata, and model governance continuously. |
| **Zero Blocking** | Runtime telemetry emitted asynchronously; Atlas consumes independently. Compute threads never blocked by governance operations. |
| **AI Governance** | Model lifecycle (weights, hyperparameters, transparency contracts) is a first-class governance citizen. |
| **Errors as Values** | `Result[S, F]` ADT across all planes. No exceptions at domain boundaries. Exhaustive pattern matching enforced by `.fold()`. |
| **Immutability** | All domain objects are frozen dataclasses. Zero mutation surface. Sealed hierarchies prevent unauthorised extension. |

---

*Architecture diagrams: [`docs/architecture-macro-v2.md`](docs/architecture-macro-v2.md) · [`docs/architecture-micro-v2.md`](docs/architecture-micro-v2.md)*

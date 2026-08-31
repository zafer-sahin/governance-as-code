# Governance-as-Code: Macro Architecture (High-Level Topology)

**Classification:** Principal Architecture Document  
**Domain:** Enterprise Data Platform — Governance-as-Code (GaC) + AI Governance  
**Version:** 2.0.0  
**Diagrams:** 1 of 2  
**Supersedes:** `architecture-macro-v1.md` (v1.0.0)

---

## Revision Summary (v1 → v2)

| # | Revision | Scope |
|---|---|---|
| R1 | Added **Discovery Engine-based Automated Data Discovery & Lineage Extraction** engine as an autonomous sub-component in Plane 3, feeding both Kafka and Atlas directly | Plane 3 |
| R2 | Extended Plane 2 Core Domain to include **Model Registry-driven Model Governance** — model weights and hyperparameters now flow through CI/CD into Model Governance policies | Plane 2 |
| R3 | Extended Plane 4 Reconciliation scope to detect and remediate drift in **Model Governance policies** alongside Ranger ACLs and Atlas TypeDefs | Plane 4 |
| R4 | Extended ADT in Plane 1 from `Result<Allow, Deny<Reason>>` to `Result<Allow, Deny<Reason> | Mask<Columns> | ModelRestrict<Constraint>>` for Private AI / Agentic AI alignment | Plane 1 |

---

## Design Principles

| Principle | Implementation |
|---|---|
| Determinism | All governance state changes are driven by versioned Git commits (GitOps) |
| Decoupling | 4 planes communicate only through well-defined protocols (REST, Kafka, Webhook) |
| Shift-Left | Governance constraints — including model security — evaluated at developer workstation before any commit |
| Drift Detection | Reconciliation Operator enforces desired state across data policies, metadata, and model governance |
| Zero Blocking | Runtime telemetry (hook-based and Discovery Engine-based) is emitted asynchronously; Atlas consumes independently |
| AI Governance | Model lifecycle (weights, hyperparameters, transparency contracts) is a first-class governance citizen |

---

## Diagram: High-Level Topology (v2)

```mermaid
graph TD
    %% -----------------------------------------------------------------
    %% PLANE 1: Shift-Left Developer Proxy — Local Validation Plane
    %% Pattern: CQRS (Query-only side) + Extended ADTs
    %% R4: ADT now includes Mask and ModelRestrict variants
    %% -----------------------------------------------------------------
    subgraph P1["PLANE 1 · Shift-Left Developer Proxy · Pattern: CQRS + Extended ADT (v2)"]
        direction TB
        DEV["Developer / Data Scientist\nIDE / Jupyter / ML Platform"]
        PROXY["Local Governance Proxy\nDry-Run Validator"]
        ADT["Extended ADT Response\nResult: Allow\n| Deny-Reason\n| Mask-Columns\n| ModelRestrict-Constraint"]

        DEV -->|"Submits Spark/Trino code\nor Model change for validation"| PROXY
        PROXY -->|"CQRS Query-only read\nEvaluates data + model constraints"| ADT
    end

    %% -----------------------------------------------------------------
    %% PLANE 2: GaC State Compiler & CI/CD Engine — Control & Build
    %% Pattern: Hexagonal Architecture + Explicit State Machine
    %% R2: Core Domain extended with Model Governance (Model Registry-driven)
    %% -----------------------------------------------------------------
    subgraph P2["PLANE 2 · GaC State Compiler & CI/CD Engine · Pattern: Hexagonal + State Machine (v2 + AI Governance)"]
        direction TB
        GH["GitHub Repository\nInbound Port — Webhook trigger"]
        CICD["GitOps CI/CD Runner\nCore Domain: Lineage, Policy Inference\nModel Monitoring + AI Transparency Contracts"]
        SM["State Machine\nCode_Analyzed to Policy_Compiled\nto Dry_Run_Passed to Policy_Enforced"]
        COMPILE["Policy Compiler\nRanger Policy-as-Code\nAtlas TypeDef IaC"]
        MODELGOV["Model Governance Compiler\nModel Registry-driven\nModel Weights + Hyperparameter Policies\nAI Transparency Contracts"]

        GH -->|"Git Webhook\nPR / Commit event\nCode or Model change"| CICD
        CICD --> SM
        SM --> COMPILE
        SM --> MODELGOV
    end

    %% -----------------------------------------------------------------
    %% PLANE 3: Federated Execution & Telemetry Bus — Data & Event
    %% Pattern: Event-Driven Architecture (Pub/Sub)
    %% R1: Added Data Discovery Engine as autonomous sub-component
    %% -----------------------------------------------------------------
    subgraph P3["PLANE 3 · Federated Execution & Telemetry Bus · Pattern: Event-Driven Pub/Sub (v2 + DiscoveryEngine)"]
        direction TB
        TRINO["Trino Engine\nFederated Query Engine\nUniversal Catalog"]
        SPARK["Apache Spark\nETL Runtime"]
        KAFKA["Kafka Topic: ATLAS_HOOK\nTelemetry Bus"]
        DISCOVERY_ENGINE["Data Discovery Engine\nAutomated Data Discovery\nLineage Extraction\nExternal Metadata Scanner"]

        TRINO -->|"Native Atlas Hook\nAsync publish"| KAFKA
        SPARK -->|"Native Atlas Hook\nAsync publish"| KAFKA
        DISCOVERY_ENGINE -->|"Async publish\nKafka ATLAS_HOOK"| KAFKA
        DISCOVERY_ENGINE -->|"Direct ingest\nAtlas REST API"| ATLAS2
        ATLAS2[/"Apache Atlas\nDirect Ingest Path"/]
    end

    %% -----------------------------------------------------------------
    %% PLANE 4: Governance Reconciliation Operator — Reconciliation Plane
    %% Pattern: Control Loop / Declarative State Matching
    %% R3: Reconciliation scope now includes Model Governance drift
    %% -----------------------------------------------------------------
    subgraph P4["PLANE 4 · Governance Reconciliation Operator · Pattern: Control Loop / Drift Detection (v2 + Model Governance)"]
        direction TB
        RECON["Kubernetes Operator\nBackground Daemon"]
        DIFF["Drift Detector\nDesired State vs Actual State\nData Policies + Model Governance"]
        REMEDIATE["Auto-Remediation\nPatch Ranger / Atlas / Model Registry"]

        RECON --> DIFF
        DIFF -->|"Drift detected\nData or Model Governance"| REMEDIATE
    end

    %% -----------------------------------------------------------------
    %% Shared Data Stores
    %% -----------------------------------------------------------------
    GIT[("Git Repository\nDesired State — Source of Truth\nCode + Policies + Model Configs")]
    ATLAS[("Apache Atlas\nMetadata Catalog\nLineage Store")]
    RANGER[("Apache Ranger\nPolicy Engine\nACL Store")]
    MREG[("Model Registry\nModel Governance Store\nWeights / Hyperparams / Contracts")]

    %% -----------------------------------------------------------------
    %% Cross-Plane Edges — Protocols Annotated
    %% -----------------------------------------------------------------

    %% P1 reads
    PROXY -->|"CQRS Read\nTrino REST API"| TRINO
    PROXY -->|"CQRS Read\nRanger REST API"| RANGER
    PROXY -->|"CQRS Read\nModel Registry REST API\nModel constraint check"| MREG

    %% Developer writes to Git
    DEV -->|"git push / PR\ncode or model change"| GIT

    %% Git to P2
    GIT -->|"Webhook Event\nHTTPS POST"| GH

    %% P2 Outbound Ports
    COMPILE -->|"Outbound Port\nRanger REST API"| RANGER
    COMPILE -->|"Outbound Port\nAtlas REST API"| ATLAS
    MODELGOV -->|"Outbound Port\nModel Registry REST API\nRegister Model Policy"| MREG

    %% P3 execution paths
    TRINO -->|"Federated SQL\nTrino Connector Protocol"| ATLAS
    KAFKA -->|"Kafka Consumer\nATLAS_HOOK topic"| ATLAS
    ATLAS2 -->|"Merge path"| ATLAS

    %% DiscoveryEngine scans external sources
    DISCOVERY_ENGINE -->|"JDBC / REST / File Scanner\nMulti-source metadata scan"| TRINO

    %% P4 reads Desired State
    GIT -->|"Git Pull / REST API"| RECON

    %% P4 reads Actual State
    RANGER -->|"Ranger REST API GET\nActual Policy State"| DIFF
    ATLAS -->|"Atlas REST API GET\nActual Lineage and Metadata"| DIFF
    MREG -->|"Model Registry REST API GET\nActual Model Governance State"| DIFF

    %% P4 remediates
    REMEDIATE -->|"Ranger REST API PUT\nPolicy Patch"| RANGER
    REMEDIATE -->|"Atlas REST API PUT\nTypeDef Patch"| ATLAS
    REMEDIATE -->|"Model Registry REST API PUT\nModel Policy Patch"| MREG
```

---

## Plane Summary (v2)

### Plane 1 — Shift-Left Developer Proxy
- **Pattern:** CQRS (Query-only side) + Extended Algebraic Data Types (ADTs)
- **v2 Change (R4):** ADT response extended from `Result<Allow, Deny<Reason>>` to a 4-variant sum type:
  ```
  Result<Allow | Deny<Reason> | Mask<Columns> | ModelRestrict<Constraint>>
  ```
  - `Mask<Columns>`: Returned when the principal may access the resource but with column-level masking applied (PII/Private AI scenarios).
  - `ModelRestrict<Constraint>`: Returned when a model change violates AI transparency contracts or Agentic AI security boundaries.
- Reads Trino catalog, Ranger policies, **and model registry governance** as the Single Source of Truth. No writes occur from this plane.

### Plane 2 — GaC State Compiler & CI/CD Engine
- **Pattern:** Hexagonal Architecture + Explicit State Machine
- **v2 Change (R2):** Core Domain now encompasses **Model Monitoring and AI Transparency Contract inference** in addition to lineage and data policy inference.
- Two Outbound Port classes:
  1. **PolicyCompiler** → Ranger (ACL) + Atlas (TypeDefs) — unchanged from v1
  2. **ModelGovernanceCompiler (Model Registry-driven)** → Model Registry — registers model weight policies, hyperparameter constraints, and AI transparency sidecar contracts
- State machine lifecycle is unchanged; both compilers are invoked in `Policy_Compiled` state and must both succeed before `Policy_Enforced`.

### Plane 3 — Federated Execution & Telemetry Bus
- **Pattern:** Event-Driven Architecture (Pub/Sub)
- **v2 Change (R1):** Added **Discovery Engine-based Automated Data Discovery & Lineage Extraction** engine as an autonomous sub-component with two emission paths:
  1. **Kafka path:** Publishes discovered lineage events to `ATLAS_HOOK` topic (same async Pub/Sub channel as native hooks)
  2. **Direct Atlas path:** For high-priority or batch discovery jobs, DiscoveryEngine can ingest directly via Atlas REST API, bypassing Kafka
- DiscoveryEngine scans external metadata sources (JDBC, REST, file-based) independently of Spark/Trino runtime execution, providing coverage for systems that have no native Atlas hook.

### Plane 4 — Governance Reconciliation Operator
- **Pattern:** Control Loop / Declarative State Matching (Drift Detection)
- **v2 Change (R3):** Drift detection scope expanded to three governance dimensions:
  1. **Ranger ACL drift** (data access policy)
  2. **Atlas TypeDef/lineage drift** (metadata governance)
  3. **Model Registry Governance drift** (AI model policy — weights, hyperparameters, transparency contracts)
- Auto-remediation now issues patches to Ranger, Atlas, **and Model Registry** REST APIs on detection.

---

*See `architecture-micro-v2.md` for the sequence-level execution and pattern flow diagram (v2).*

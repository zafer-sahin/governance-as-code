# Governance-as-Code: Macro Architecture (High-Level Topology)

**Classification:** Principal Architecture Document  
**Domain:** Enterprise Data Platform — Governance-as-Code (GaC)  
**Version:** 1.0.0  
**Diagrams:** 1 of 2

---

## Scope

This document defines the **High-Level Topology** of the Governance-as-Code system. The architecture implements a deterministic, declarative governance paradigm over Apache Atlas and Apache Ranger, with Trino acting as the Universal Catalog (Federated Query Engine) and Apache Spark as the ETL execution runtime. The system is decomposed into **4 orthogonal, fully decoupled planes**. Each plane carries explicit software design pattern annotations.

---

## Design Principles

| Principle | Implementation |
|---|---|
| Determinism | All governance state changes are driven by versioned Git commits (GitOps) |
| Decoupling | 4 planes communicate only through well-defined protocols (REST, Kafka, Webhook) |
| Shift-Left | Governance constraints evaluated at developer workstation before any commit |
| Drift Detection | Reconciliation Operator continuously enforces desired state against actual state |
| Zero Blocking | Runtime telemetry is emitted asynchronously via Kafka; Atlas consumes independently |

---

## Diagram: High-Level Topology

```mermaid
graph TD
    %% -----------------------------------------------------------------
    %% PLANE 1: Shift-Left Developer Proxy — Local Validation Plane
    %% Pattern: CQRS (Query-only side) + ADTs
    %% -----------------------------------------------------------------
    subgraph P1["PLANE 1 · Shift-Left Developer Proxy · Pattern: CQRS + ADT"]
        direction TB
        DEV["Developer\nIDE / Jupyter / ML Platform"]
        PROXY["Local Governance Proxy\nDry-Run Validator"]
        ADT["ADT Response\nResult-Allow or Deny-Reason"]

        DEV -->|"Submits Spark/Trino code\nfor local validation"| PROXY
        PROXY -->|"CQRS Query-only read"| ADT
    end

    %% -----------------------------------------------------------------
    %% PLANE 2: GaC State Compiler & CI/CD Engine — Control & Build
    %% Pattern: Hexagonal Architecture + Explicit State Machine
    %% -----------------------------------------------------------------
    subgraph P2["PLANE 2 · GaC State Compiler & CI/CD Engine · Pattern: Hexagonal + State Machine"]
        direction TB
        GH["GitHub Repository\nInbound Port — Webhook trigger"]
        CICD["GitOps CI/CD Runner\nCore Domain: Lineage and Policy Inference"]
        SM["State Machine\nCode_Analyzed to Policy_Compiled\nto Dry_Run_Passed to Policy_Enforced"]
        COMPILE["Policy Compiler\nRanger Policy-as-Code\nAtlas TypeDef IaC"]

        GH -->|"Git Webhook\nPR / Commit event"| CICD
        CICD --> SM
        SM --> COMPILE
    end

    %% -----------------------------------------------------------------
    %% PLANE 3: Federated Execution & Telemetry Bus — Data & Event
    %% Pattern: Event-Driven Architecture (Pub/Sub)
    %% -----------------------------------------------------------------
    subgraph P3["PLANE 3 · Federated Execution & Telemetry Bus · Pattern: Event-Driven Pub/Sub"]
        direction TB
        TRINO["Trino Engine\nFederated Query Engine\nUniversal Catalog"]
        SPARK["Apache Spark\nETL Runtime"]
        KAFKA["Kafka Topic: ATLAS_HOOK\nTelemetry Bus"]

        TRINO -->|"Native Atlas Hook\nAsync publish"| KAFKA
        SPARK -->|"Native Atlas Hook\nAsync publish"| KAFKA
    end

    %% -----------------------------------------------------------------
    %% PLANE 4: Governance Reconciliation Operator — Reconciliation Plane
    %% Pattern: Control Loop / Declarative State Matching
    %% -----------------------------------------------------------------
    subgraph P4["PLANE 4 · Governance Reconciliation Operator · Pattern: Control Loop / Drift Detection"]
        direction TB
        RECON["Kubernetes Operator\nBackground Daemon"]
        DIFF["Drift Detector\nDesired State vs Actual State"]
        REMEDIATE["Auto-Remediation\nPatch Ranger / Atlas"]

        RECON --> DIFF
        DIFF -->|"Drift detected"| REMEDIATE
    end

    %% -----------------------------------------------------------------
    %% Shared Data Stores
    %% -----------------------------------------------------------------
    GIT[("Git Repository\nDesired State — Source of Truth")]
    ATLAS[("Apache Atlas\nMetadata Catalog\nLineage Store")]
    RANGER[("Apache Ranger\nPolicy Engine\nACL Store")]

    %% -----------------------------------------------------------------
    %% Cross-Plane Edges — Protocols Annotated
    %% -----------------------------------------------------------------

    PROXY -->|"CQRS Read\nTrino REST API"| TRINO
    PROXY -->|"CQRS Read\nRanger REST API"| RANGER
    DEV -->|"git push / PR"| GIT
    GIT -->|"Webhook Event\nHTTPS POST"| GH
    COMPILE -->|"Outbound Port\nRanger REST API"| RANGER
    COMPILE -->|"Outbound Port\nAtlas REST API"| ATLAS
    TRINO -->|"Federated SQL\nTrino Connector Protocol"| ATLAS
    KAFKA -->|"Kafka Consumer\nATLAS_HOOK topic"| ATLAS
    GIT -->|"Git Pull / REST API"| RECON
    RANGER -->|"Ranger REST API GET\nActual Policy State"| DIFF
    ATLAS -->|"Atlas REST API GET\nActual Lineage State"| DIFF
    REMEDIATE -->|"Ranger REST API PUT\nPolicy Patch"| RANGER
    REMEDIATE -->|"Atlas REST API PUT\nTypeDef Patch"| ATLAS
```

---

## Plane Summary

### Plane 1 — Shift-Left Developer Proxy
- **Pattern:** CQRS (Query-only side) + Algebraic Data Types (ADTs)
- Provides deterministic dry-run responses of the form `Result<Allow, Deny<Reason>>` before any code reaches CI/CD.
- Reads Trino catalog and Ranger policies as the **Single Source of Truth**. No writes occur from this plane.

### Plane 2 — GaC State Compiler & CI/CD Engine
- **Pattern:** Hexagonal Architecture + Explicit State Machine
- GitHub acts as the **Inbound Port**. Ranger/Atlas REST APIs act as **Outbound Ports**.
- Core domain (lineage/policy inference) is fully isolated from infrastructure concerns.
- State machine enforces a deterministic PR lifecycle: `Code_Analyzed → Policy_Compiled → Dry_Run_Passed → Policy_Enforced`.

### Plane 3 — Federated Execution & Telemetry Bus
- **Pattern:** Event-Driven Architecture (Pub/Sub)
- Native Trino/Spark Atlas hooks publish lineage events to `ATLAS_HOOK` Kafka topic **asynchronously**, eliminating compute blocking.
- Atlas consumes at its own pace — decoupled ingestion.

### Plane 4 — Governance Reconciliation Operator
- **Pattern:** Control Loop / Declarative State Matching (Drift Detection)
- Continuously compares **Desired State** (Git) with **Actual State** (Ranger + Atlas).
- On drift detection, auto-remediates by patching Ranger ACLs and Atlas TypeDefs.

---

*See `architecture-micro-v1.md` for the sequence-level execution and pattern flow diagram.*

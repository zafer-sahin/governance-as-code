# Governance-as-Code: Micro Architecture (Low-Level Interaction Flow)

**Classification:** Principal Architecture Document  
**Domain:** Enterprise Data Platform — Governance-as-Code (GaC)  
**Version:** 1.0.0  
**Diagrams:** 2 of 2

---

## Scope

This document defines the **Low-Level Interaction Flow** for the complete lifecycle of a developer change to a Spark/Trino pipeline under the GaC system. The sequence diagram maps every actor-to-actor message, annotates each step with its governing software design pattern, and includes the asynchronous Kafka telemetry path and the Reconciliation Operator's eventual-consistency verification loop.

---

## Actors

| Actor | Plane | Role |
|---|---|---|
| `Developer` | P1 | Data Scientist / Engineer (IDE, Jupyter, ML Platform) |
| `LocalProxy` | P1 | Governance dry-run proxy — CQRS Query side |
| `TrinoEngine` | P1 / P3 | Universal Catalog + Federated Query Engine |
| `RangerAPI` | P1 / P2 / P4 | Apache Ranger REST endpoint |
| `GitHub` | P2 | Git repository — Inbound Port (Hexagonal) |
| `CICDRunner` | P2 | GitOps runner — Hexagonal Core Domain |
| `StateMachine` | P2 | PR lifecycle enforcer |
| `PolicyCompiler` | P2 | Outbound Port — Ranger/Atlas provisioner |
| `AtlasAPI` | P2 / P3 / P4 | Apache Atlas REST endpoint |
| `SparkEngine` | P3 | ETL Runtime with native Atlas hook |
| `KafkaBus` | P3 | `ATLAS_HOOK` topic — async telemetry bus |
| `ReconOperator` | P4 | Kubernetes Operator — Control Loop |

---

## Diagram: Low-Level Execution and Pattern Flow

```mermaid
sequenceDiagram
    autonumber

    participant DEV as Developer
    participant PROXY as LocalProxy<br/>[P1: CQRS Query Side]
    participant TRINO as TrinoEngine<br/>[P1/P3: Universal Catalog]
    participant RANGER as RangerAPI<br/>[P1/P2/P4: Policy Engine]
    participant GH as GitHub<br/>[P2: Inbound Port]
    participant CICD as CICDRunner<br/>[P2: Hexagonal Core Domain]
    participant SM as StateMachine<br/>[P2: PR Lifecycle]
    participant COMPILE as PolicyCompiler<br/>[P2: Outbound Port]
    participant ATLAS as AtlasAPI<br/>[P2/P3/P4: Metadata Catalog]
    participant SPARK as SparkEngine<br/>[P3: ETL Runtime]
    participant KAFKA as KafkaBus<br/>[P3: ATLAS_HOOK Topic]
    participant RECON as ReconOperator<br/>[P4: Control Loop]

    %% ================================================================
    %% PHASE 1: LOCAL DRY-RUN — Pattern: CQRS + ADT
    %% ================================================================
    Note over DEV,PROXY: PHASE 1 · Local Dry-Run · Pattern: CQRS + Algebraic Data Types

    DEV->>PROXY: submit(spark_code | trino_query)
    Note right of PROXY: CQRS: Query-only side activated.<br/>No writes permitted from this plane.

    PROXY->>TRINO: CQRS Read — catalog.getSchema(dataset)
    TRINO-->>PROXY: SchemaMetadata{columns, classifications, owners}

    PROXY->>RANGER: CQRS Read — policy.evaluate(principal, resource, action)
    RANGER-->>PROXY: PolicyDecision{ALLOW | DENY, rule_id, reason}

    Note right of PROXY: ADT evaluation:<br/>Result = Allow if PolicyDecision==ALLOW<br/>Result = Deny(reason) if PolicyDecision==DENY

    PROXY-->>DEV: ADT Result: Allow | Deny(reason)

    alt Result == Deny(reason)
        DEV->>DEV: Fix code locally. Retry dry-run.
    end

    %% ================================================================
    %% PHASE 2: GIT PUSH & WEBHOOK — Pattern: Hexagonal (Inbound Port)
    %% ================================================================
    Note over DEV,SM: PHASE 2 · Git Push and Webhook · Pattern: Hexagonal Architecture

    DEV->>GH: git push / open PR
    Note right of GH: Hexagonal: GitHub is the Inbound Port.<br/>External trigger enters the Core Domain here.

    GH->>CICD: Webhook POST /events {pr_id, commit_sha, diff}
    Note right of CICD: Core Domain receives raw event.<br/>Infrastructure concern (HTTP) is fully isolated.

    %% ================================================================
    %% PHASE 3: CI/CD CORE DOMAIN — Pattern: Hexagonal + State Machine
    %% ================================================================
    Note over CICD,COMPILE: PHASE 3 · CI/CD Core Domain · Pattern: Hexagonal Core + Explicit State Machine

    CICD->>SM: transition(PR, event=WEBHOOK_RECEIVED)
    SM-->>CICD: State: Code_Analyzed

    CICD->>CICD: inferLineage(diff) + inferPolicies(diff)
    CICD->>SM: transition(PR, event=INFERENCE_COMPLETE)
    SM-->>CICD: State: Policy_Compiled

    CICD->>PROXY: triggerCIDry-Run(compiled_policies, pr_id)
    PROXY->>RANGER: CQRS Read — policy.evaluate(compiled_policies)
    RANGER-->>PROXY: PolicyDecision{ALLOW}
    PROXY-->>CICD: DryRunResult: Allow

    CICD->>SM: transition(PR, event=DRY_RUN_PASSED)
    SM-->>CICD: State: Dry_Run_Passed

    Note right of SM: State Machine gate: pipeline halts<br/>if any state transition is invalid.

    %% ================================================================
    %% PHASE 4: OUTBOUND PORT PROVISIONING — Pattern: Hexagonal (Outbound Ports)
    %% ================================================================
    Note over COMPILE,ATLAS: PHASE 4 · Outbound Port Provisioning · Pattern: Hexagonal Outbound Ports

    CICD->>COMPILE: provision(ranger_policies, atlas_typedefs)
    Note right of COMPILE: Hexagonal: PolicyCompiler is the Outbound Port.<br/>Core Domain delegates infrastructure writes here.

    COMPILE->>RANGER: REST PUT /policies {policy_payload}
    RANGER-->>COMPILE: 200 OK {policy_id, version}

    COMPILE->>ATLAS: REST PUT /typedefs {typedef_payload}
    ATLAS-->>COMPILE: 200 OK {typedef_guid, version}

    COMPILE-->>CICD: ProvisioningResult{ranger_policy_id, atlas_guid}
    CICD->>SM: transition(PR, event=PROVISIONING_COMPLETE)
    SM-->>CICD: State: Policy_Enforced

    CICD->>GH: PR Status: APPROVED + merge(pr_id)

    %% ================================================================
    %% PHASE 5: FEDERATED EXECUTION — Pattern: Event-Driven (Pub/Sub)
    %% ================================================================
    Note over SPARK,KAFKA: PHASE 5 · Federated Execution · Pattern: Event-Driven Architecture Pub/Sub

    DEV->>SPARK: submit(merged_spark_job)
    SPARK->>TRINO: Federated SQL query via Trino Connector
    TRINO-->>SPARK: QueryResult{rows, schema}

    Note right of SPARK: Native Atlas Hook activates on job completion.<br/>Async emit — compute thread NOT blocked.

    SPARK-)KAFKA: async publish → ATLAS_HOOK {lineage_event, job_id, timestamp}
    TRINO-)KAFKA: async publish → ATLAS_HOOK {query_lineage, session_id, timestamp}

    Note right of KAFKA: Pub/Sub decoupling:<br/>Producers (Spark/Trino) emit fire-and-forget.<br/>Atlas consumes independently at its own pace.

    KAFKA-)ATLAS: async consume → ingestLineage(lineage_event)
    ATLAS-->>ATLAS: persist(LineageGraph, DatasetClassifications)

    %% ================================================================
    %% PHASE 6: RECONCILIATION LOOP — Pattern: Control Loop / Drift Detection
    %% ================================================================
    Note over RECON,ATLAS: PHASE 6 · Reconciliation Loop · Pattern: Control Loop / Declarative State Matching

    loop Every N seconds — Control Loop tick
        RECON->>GH: REST GET /contents/policies {ref=main}
        GH-->>RECON: DesiredState{ranger_policies[], atlas_typedefs[]}

        RECON->>RANGER: REST GET /policies {service=all}
        RANGER-->>RECON: ActualPolicies{policy_list[]}

        RECON->>ATLAS: REST GET /entities {type=all}
        ATLAS-->>RECON: ActualMetadata{entity_list[], lineage_graph}

        RECON->>RECON: diff(DesiredState, ActualState)

        alt Drift Detected — Ranger policy mismatch
            Note right of RECON: Drift detected: Actual policy diverges<br/>from Desired State in Git.
            RECON->>RANGER: REST PUT /policies {remediated_payload}
            RANGER-->>RECON: 200 OK — Policy patched
        end

        alt Drift Detected — Atlas typedef or lineage mismatch
            Note right of RECON: Drift detected: Atlas metadata diverges<br/>from declared TypeDefs.
            RECON->>ATLAS: REST PUT /typedefs {remediated_payload}
            ATLAS-->>RECON: 200 OK — TypeDef patched
        end

        alt No Drift
            Note right of RECON: Actual State == Desired State.<br/>No remediation required. Loop continues.
        end
    end
```

---

## Pattern Reference by Phase

| Phase | Pattern | Structural Role |
|---|---|---|
| 1 — Local Dry-Run | **CQRS (Query side)** | Separates read path from write path. LocalProxy issues queries only; never mutates state. |
| 1 — Local Dry-Run | **Algebraic Data Types (ADTs)** | `Result<Allow, Deny<Reason>>` is a sum type. Response is total, exhaustive, and unambiguous. |
| 2 — Git Push | **Hexagonal Architecture (Inbound Port)** | GitHub Webhook is the external driver. The Core Domain is agnostic to the transport mechanism. |
| 3 — CI/CD Core | **Explicit State Machine** | PR lifecycle is a finite automaton. Invalid transitions are rejected, preventing out-of-order execution. |
| 4 — Provisioning | **Hexagonal Architecture (Outbound Port)** | PolicyCompiler adapts Core Domain output to Ranger/Atlas REST APIs. The Core Domain never speaks HTTP directly. |
| 5 — Execution | **Event-Driven Architecture (Pub/Sub)** | Fire-and-forget telemetry via Kafka. Producers and Atlas consumer are temporally and spatially decoupled. |
| 6 — Reconciliation | **Control Loop / Declarative State Matching** | Operator continuously reconciles Git-declared desired state against runtime actual state. Drift triggers auto-remediation. |

---

## State Machine Transitions (Plane 2)

```
[Code_Analyzed] ──INFERENCE_COMPLETE──► [Policy_Compiled]
[Policy_Compiled] ──DRY_RUN_PASSED──► [Dry_Run_Passed]
[Dry_Run_Passed] ──PROVISIONING_COMPLETE──► [Policy_Enforced]

Any invalid event in any state → REJECTED (pipeline blocked, PR not merged)
```

---

## Async Boundaries

Two explicit async boundaries exist in the system, both in Plane 3:

1. **Spark/Trino → Kafka (`ATLAS_HOOK`):** Fire-and-forget. Native hooks run on a separate thread pool. Query/job execution is never blocked waiting for telemetry acknowledgement.
2. **Kafka → Atlas:** Atlas consumes the `ATLAS_HOOK` topic independently. Backpressure and consumer lag are Atlas-internal concerns; producers are unaffected.

---

*See `architecture-macro-v1.md` for the macro-level plane topology and protocol annotations.*

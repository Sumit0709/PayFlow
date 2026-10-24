# PayFlow — Project Master Document

> **Purpose of this document:** This is a complete reference for the PayFlow project. It covers the developer's background, goals, architecture, services, AI integration, infrastructure, CI/CD, and phased implementation plan. Any AI tool or developer reading this document should be able to understand the full scope of the project and assist with any phase of implementation.

---

## 1. Developer Profile & Context

| Field | Detail |
|---|---|
| Experience | 3 years as a Java Software Engineer |
| Primary Language | Java (Spring Boot) |
| Secondary Skills | JavaScript, React, Next.js |
| Domain | Fintech / Banking |
| Goal | Gain hands-on practical experience in microservices, DevOps, CI/CD, Kubernetes, and AI integration to match and justify resume claims |

### Resume Claims to Validate via This Project

The developer's resume includes the following claims. The project is designed so that the developer can speak to all of these from real hands-on experience:

1. **REST APIs and microservices for card and direct debit transaction processing** — supporting 10M+ customers at 99.98% availability.
2. **Kafka-based notification microservice** — processing 300K+ events/day with sub-150ms latency.
3. **Unit and integration tests** — reducing fraud disputes by 30%.
4. **Automated customer data ingestion service** — integrating multiple internal systems, rule-based segmentation.
5. **Modules for Offers, Consents, and Loyalty** — Agile team, data governance compliance.

---

## 2. Project Overview

**Project Name:** PayFlow
**Domain:** Finance / Payments
**Type:** Microservices-based Payment Processing Platform (inspired by internals of Razorpay / Stripe)

### What PayFlow Does

PayFlow is a backend payment processing platform that allows users to:
- Register and authenticate
- Create and manage bank accounts with GDPR-compliant consent tracking
- Initiate card and direct debit transactions with balance verification and concurrency safety
- Have transactions automatically screened for fraud using an LLM (AI-assisted) with rule-based fallback
- Receive fast template-based alerts for normal transactions, and AI-personalised alerts for flagged/blocked ones
- Have internal account data synced and segmented automatically via a scheduled ingestion service

PayFlow is **not** a frontend application. It is a set of backend microservices communicating via REST and Kafka, deployed on Kubernetes, with a full CI/CD pipeline via Harness.

---

## 3. Goals

| Goal | Detail |
|---|---|
| Microservices design | Bounded contexts, service decomposition, independent schemas |
| Kafka | Event-driven architecture, producer/consumer, partitioning, DLT, deduplication |
| AI / LLM integration | OpenAI API for fraud scoring and personalised notifications (FLAGGED/BLOCKED only) |
| Resilience | Circuit breaker (Resilience4j), Transactional Outbox, @RetryableTopic |
| Testing | JUnit 5 + Mockito + Testcontainers wired from Week 1 |
| Docker | Containerise every service with memory-capped JVM |
| Kubernetes | Self-managed cluster on AWS EC2, HPA, probes, zero-downtime deploys |
| CI/CD | Harness pipeline with path-based triggers, per-service build |
| Monitoring | Prometheus + Grafana dashboards |
| Data Governance | GDPR consent tracking, loyalty/offer segmentation |

---

## 4. Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3 |
| Build Tool | Maven (multi-module project) |
| Messaging | Apache Kafka |
| Database | PostgreSQL — single instance, 6 logical schemas (one per service) |
| Monetary Type | `BigDecimal` in Java / `NUMERIC(19,4)` in all DB columns |
| AI / LLM | OpenAI API — GPT-4o-mini |
| Resilience | Resilience4j (circuit breaker, fallback), `@RetryableTopic` (Spring Kafka) |
| Service Discovery | Kubernetes DNS (no Eureka needed) |
| API Gateway | Spring Cloud Gateway |
| Auth | JWT (issued by user-service, validated at gateway) |
| Containerisation | Docker (JVM memory capped via `JAVA_TOOL_OPTIONS`) |
| Container Registry | AWS ECR |
| Orchestration | Kubernetes — self-managed on AWS EC2 using kubeadm |
| CI/CD | Harness Free Tier (path-based triggers, per-service pipelines) |
| Monitoring | Prometheus + Grafana |
| Testing | JUnit 5, Mockito, Testcontainers |

---

## 5. Microservices

```
payflow/
├── api-gateway/            # Spring Cloud Gateway — routing, JWT auth filter, rate limiting
├── user-service/           # User registration, login, JWT, GDPR consent endpoints
├── account-service/        # Bank accounts, balance debit with optimistic locking
├── transaction-service/    # Core: fraud check → balance debit → outbox → Kafka publish
├── fraud-service/          # LLM risk scoring with rule-based fallback
├── notification-service/   # Kafka consumer — fast templates + async LLM for FLAGGED/BLOCKED
└── ingestion-service/      # Scheduled data sync + loyalty/offer segmentation rules
```

### Service Responsibilities

#### api-gateway
- Entry point for all client requests
- Routes requests to downstream services
- Validates JWT tokens via a global filter
- Rate limiting per user/IP

#### user-service
- `POST /api/users/register` — register a new user
- `POST /api/users/login` — authenticate and return JWT
- `POST /api/users/{userId}/consents` — record a user's data consent (GDPR)
- `GET  /api/users/{userId}/consents` — list active consents for a user
- `DELETE /api/users/{userId}/consents/{consentId}` — withdraw consent
- Owns: `userdb` schema — tables: `users`, `user_consents`

**user_consents table:**
```sql
CREATE TABLE user_consents (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID NOT NULL REFERENCES users(id),
    consent_type  VARCHAR(50) NOT NULL,  -- MARKETING, ANALYTICS, DATA_SHARING
    granted       BOOLEAN NOT NULL DEFAULT TRUE,
    granted_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    withdrawn_at  TIMESTAMP,
    ip_address    VARCHAR(45),
    version       INTEGER NOT NULL DEFAULT 0  -- optimistic lock
);
```
> Validates resume claim #5: data governance and compliance.

#### account-service
- `POST /api/accounts` — create a bank account for a user
- `GET  /api/accounts/{userId}` — fetch accounts
- `GET  /api/accounts/{accountId}/balance` — get balance
- `POST /api/accounts/{accountId}/debit` — **atomically verify and deduct balance** (called by transaction-service)
- Owns: `accountdb` schema — table: `accounts`

**Balance debit uses JPA `@Version` optimistic locking:**
```java
@Entity
public class Account {
    @Version
    private Integer version;  // JPA increments on every update

    @Column(precision = 19, scale = 4)
    private BigDecimal balance;  // NUMERIC(19,4) in DB — never float/double for money
}
```
If two transactions try to debit the same account simultaneously, one gets an `ObjectOptimisticLockingFailureException` → transaction-service catches this and returns HTTP 409 Conflict. This prevents double-spend.

#### transaction-service
- `POST /api/transactions` — initiate a card or direct debit transaction
- **Full processing flow (in order):**

```
1. Check idempotency key — if duplicate, return cached response immediately

2. Calculate fraud context (read-only DB queries, no money touched yet):
   → Query own DB: 30-day average spend for this user
   → Query own DB: transaction count in last 1 hour for this user

3. Call fraud-service POST /api/fraud/evaluate (sync, with fraud context payload)
   → BLOCK decision → return 403 immediately. No debit ever attempted.
   → APPROVE or FLAG decision → continue to step 4

4. Call account-service POST /api/accounts/{id}/debit
   → If 402 (insufficient funds) → return 402 to client
   → If 409 (optimistic lock conflict) → retry once, then return 409 to client
   → Success → continue to step 5

5. Write transaction record + outbox event atomically in one DB transaction
   (status = SUCCESS if APPROVE, status = FLAGGED if FLAG)

6. Outbox poller reads outbox_events table → publishes to Kafka topic transaction.events
```

> **Why fraud-before-debit?** Running fraud check first (Steps 2–3) is read-only — no money moves. If the decision is BLOCK, we return 403 immediately and never touch the account. This eliminates the need for any compensating credit call over the network, which could itself fail. Money is only debited (Step 4) once we know the transaction is not blocked.

- Owns: `transactiondb` schema — tables: `transactions`, `outbox_events`

**Fraud context enrichment — what transaction-service calculates and sends:**
```json
POST /api/fraud/evaluate
{
  "transactionId": "uuid",
  "userId": "uuid",
  "amount": 24000.0000,
  "currency": "INR",
  "merchantName": "Amazon",
  "merchantCategory": "E_COMMERCE",
  "transactionType": "CARD",
  "timestamp": "2024-01-15T10:30:00Z",
  "userAvgSpend30Days": 8500.0000,
  "transactionCountLastHour": 2
}
```

#### fraud-service
- `POST /api/fraud/evaluate` — called synchronously by transaction-service
- **Primary path:** sends enriched transaction context to OpenAI API (GPT-4o-mini)
- **Fallback path:** Resilience4j circuit breaker triggers rule-based engine if OpenAI fails or exceeds 3s
- Returns: `{ riskScore: int, decision: APPROVE | FLAG | BLOCK, reason: String }`
- Persists every evaluation to `fraud_evaluations` table for audit trail
- Owns: `frauddb` schema — table: `fraud_evaluations`

**Risk Score Thresholds:**
| Score | Decision |
|---|---|
| 0–39 | APPROVE |
| 40–70 | FLAG (transaction allowed but marked suspicious) |
| 71–100 | BLOCK |

**Rule-based fallback checks (when OpenAI unavailable):**
- Amount > ₹50,000
- More than 5 transactions in last 1 hour (uses `transactionCountLastHour` from payload)
- Amount > 3× `userAvgSpend30Days`
- Merchant on blacklist

#### notification-service
- Consumes `TransactionEvent` from Kafka topic `transaction.events`
- **Two-tier alert strategy to meet sub-150ms SLA:**

| Transaction Status | Alert Path | Latency |
|---|---|---|
| `SUCCESS` | Fast template (no LLM call) | < 50ms |
| `FLAGGED` | Async GPT-4o-mini call | 150–500ms (acceptable, async) |
| `BLOCKED` | Async GPT-4o-mini call | 150–500ms (acceptable, async) |

**Fast template example (SUCCESS):**
```
Hi {firstName}, ₹{amount} paid to {merchantName} on {date}. 
Ref: {transactionId}.
```

**LLM path (FLAGGED/BLOCKED only) — async, does not block consumer:**
```java
@KafkaListener(topics = "transaction.events", groupId = "notification-group")
public void consume(TransactionEvent event) {
    if (event.getStatus() == SUCCESS) {
        sendTemplateAlert(event);          // < 50ms, sync
    } else {
        sendLlmAlertAsync(event);          // async, non-blocking
    }
}
```

**Kafka reliability — `@RetryableTopic` + Dead Letter Topic:**
```java
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2),
    dltTopicSuffix = ".DLT"
)
@KafkaListener(topics = "transaction.events")
public void consume(TransactionEvent event) { ... }
```
- Failed events retry 3 times with exponential backoff
- After 3 failures → routed to `transaction.events.DLT` (Dead Letter Topic)
- DLT consumer logs and alerts for manual investigation

**Deduplication via `eventId`:**
```java
// Before processing, check if eventId already handled
if (processedEventRepository.existsByEventId(event.getEventId())) {
    log.warn("Duplicate event {}, skipping", event.getEventId());
    return;
}
```

- Owns: `notificationdb` schema — tables: `notifications`, `processed_events`

#### ingestion-service
- Scheduled `@Scheduled` job — runs every 15 minutes
- Calls account-service and transaction-service internal APIs
- **Rule-based loyalty/offer segmentation (validates resume claim #5):**

```
Segment rules applied after each ingestion run:
  HIGH_SPENDER     → total spend last 30 days > ₹1,00,000
  FREQUENT_USER    → transaction count last 7 days > 20
  FRAUD_RISK       → FLAGGED transactions in last 30 days > 2
  ELIGIBLE_OFFER   → HIGH_SPENDER AND NOT FRAUD_RISK
  LOYALTY_TIER     → PLATINUM (>₹5L/year), GOLD (>₹2L/year), SILVER (rest)
```

- Segments are persisted to `customer_segments` table in ingestiondb
- Owns: `ingestiondb` schema — tables: `ingestion_logs`, `customer_segments`

---

## 6. Architecture Diagram

```
Client (Postman / Frontend)
           │
           ▼
  ┌─────────────────────┐
  │     API Gateway      │  ← JWT validation, rate limiting, routing
  └─────────────────────┘
           │
    ┌──────┴──────────────────────────────┐
    │              │                      │
    ▼              ▼                      ▼
User Service  Account Service      Transaction Service
(+ consents)  (optimistic lock     │
               balance debit)      ├─1─► Calculate Fraud Context (read-only DB)
                                   │     (30-day avg spend, 1-hour count)
                                   │
                                   ├─2─► Fraud Service (sync REST)
                                   │     enriched context payload
                                   │         │
                                   │    ┌────┴────────────┐
                                   │    │                  │
                                   │  OpenAI API     Rule-based    BLOCK → 403
                                   │  (Primary)      Fallback      (no debit)
                                   │
                                   ├─3─► Account Service (only if APPROVE or FLAG)
                                   │     POST /debit (balance check + deduct)
                                   │
                                   └─4─► Outbox Table (atomic write)
                                              │
                                         Outbox Poller
                                              │
                                         Kafka Topic: transaction.events
                                              │
                                    ┌─────────┴──────────┐
                                    │                     │
                              SUCCESS event         FLAGGED/BLOCKED
                              Fast Template          Async LLM
                              (< 50ms)              GPT-4o-mini
                                    │                     │
                               Email/SMS (mock)     Email/SMS (mock)
                                    │
                              @RetryableTopic → DLT on failure

Ingestion Service (scheduled every 15 min)
  ├── Calls Account Service + Transaction Service APIs
  └── Applies segmentation rules → customer_segments table
```

---

## 7. Transactional Outbox Pattern

**Problem it solves:** Without outbox, a crash between "save transaction to DB" and "publish to Kafka" causes data loss — DB has the record but Kafka never got the event, so notification-service never fires.

**How it works:**

```
transaction-service DB transaction (atomic):
  INSERT INTO transactions (...)       ← save transaction
  INSERT INTO outbox_events (...)      ← save unpublished event
  COMMIT

Outbox Poller (separate scheduled thread, every 500ms):
  SELECT * FROM outbox_events WHERE published = false
  FOR EACH event:
    → publish to Kafka
    → UPDATE outbox_events SET published = true
```

**outbox_events table:**
```sql
CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id       UUID NOT NULL UNIQUE,       -- used for deduplication
    aggregate_id   UUID NOT NULL,              -- transactionId
    event_type     VARCHAR(50) NOT NULL,       -- TRANSACTION_COMPLETED
    payload        JSONB NOT NULL,             -- full TransactionEvent JSON
    published      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMP
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(published) WHERE published = false;
```

---

## 8. Kafka Design

| Property | Value |
|---|---|
| Topic | `transaction.events` |
| Dead Letter Topic | `transaction.events.DLT` |
| Producer | transaction-service (via Outbox Poller) |
| Consumer | notification-service |
| Partitions | 3 (allows parallel consumers) |
| Consumer Group | `notification-group` |
| Retry | `@RetryableTopic` — 3 attempts, exponential backoff |
| Deduplication | `eventId` checked against `processed_events` table |

### TransactionEvent Payload (JSON)

```json
{
  "eventId": "uuid",
  "userId": "uuid",
  "accountId": "uuid",
  "transactionId": "uuid",
  "amount": 24000.0000,
  "currency": "INR",
  "merchantName": "Amazon",
  "transactionType": "CARD | DIRECT_DEBIT",
  "status": "SUCCESS | FLAGGED | BLOCKED",
  "fraudScore": 45,
  "fraudReason": "Amount exceeds user 30-day average by 3x",
  "userFirstName": "Rahul",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

## 9. AI / LLM Integration

### 9.1 Fraud Service — OpenAI Integration

**Model:** GPT-4o-mini
**Call type:** Synchronous REST call from fraud-service to OpenAI API
**Input:** Enriched payload calculated by transaction-service (30-day avg spend, 1-hour count)

**Sample Prompt:**
```
You are a fraud detection engine for a banking application.
Evaluate the following transaction and return a JSON response only.

Transaction Details:
- User ID: {userId}
- Amount: ₹{amount}
- Merchant: {merchantName}
- Transaction Type: {type}
- Time: {timestamp}
- User's average transaction amount (last 30 days): ₹{userAvgSpend30Days}
- Transactions in last 1 hour: {transactionCountLastHour}

Return ONLY this JSON, no explanation:
{
  "riskScore": <integer 0-100>,
  "reason": "<one sentence explanation>"
}
```

**Circuit Breaker (Resilience4j):**
- OpenAI timeout: 3 seconds
- On failure/timeout → rule-based fallback activates automatically

### 9.2 Notification Service — OpenAI Integration

**Model:** GPT-4o-mini
**Call type:** Async (non-blocking) — only for FLAGGED and BLOCKED transactions
**SUCCESS transactions:** fast template, no LLM call

**Sample Prompt (FLAGGED/BLOCKED only):**
```
You are a banking notification assistant. Write a short, friendly,
personalised alert for the following transaction.
Keep it under 2 sentences. Include a gentle warning and call to action.

Transaction: ₹{amount} to {merchantName}
Status: {status}
Fraud Reason: {fraudReason}
User's first name: {firstName}

Return ONLY the message text.
```

**Sample Output:**
```
Hi Rahul, a ₹24,000 payment to Amazon has been flagged as suspicious.
If this wasn't you, please contact support immediately.
```

---

## 10. Database Design

**Infrastructure:** Single PostgreSQL instance with 6 separate logical schemas (one per service). This applies **both locally and on K8s** — one Postgres process, consistent behaviour across environments, and dramatically lower RAM usage vs running 6 separate Postgres containers.

**Local dev (`docker-compose-infra.yml`)** uses one `postgres:16-alpine` container with an `init-schemas.sql` script mounted at `/docker-entrypoint-initdb.d/` that creates all 6 schemas and grants per-schema credentials on first start. This mirrors the K8s setup exactly — no environment drift.

**`init-schemas.sql` (mounted into the single Postgres container):**
```sql 
CREATE USER payflow_user  WITH PASSWORD 'payflow123';
CREATE USER payflow_account WITH PASSWORD 'payflow123';
CREATE USER payflow_txn   WITH PASSWORD 'payflow123';
CREATE USER payflow_fraud WITH PASSWORD 'payflow123';
CREATE USER payflow_notif WITH PASSWORD 'payflow123';
CREATE USER payflow_ingest WITH PASSWORD 'payflow123';

CREATE SCHEMA userdb     AUTHORIZATION payflow_user;
CREATE SCHEMA accountdb  AUTHORIZATION payflow_account;
CREATE SCHEMA transactiondb AUTHORIZATION payflow_txn;
CREATE SCHEMA frauddb    AUTHORIZATION payflow_fraud;
CREATE SCHEMA notificationdb AUTHORIZATION payflow_notif;
CREATE SCHEMA ingestiondb AUTHORIZATION payflow_ingest;
```

Each service's `application.yml` sets `spring.datasource.url` to point at its own schema:
```
jdbc:postgresql://postgres:5432/payflowdb?currentSchema=userdb
```

**Monetary values:** All money columns use `NUMERIC(19,4)` in PostgreSQL and `BigDecimal` in Java. Never `float`, `double`, or `DECIMAL(10,2)` for financial data.

### Schema Summary

| Service | Schema | Key Tables |
|---|---|---|
| user-service | `userdb` | `users`, `user_consents` |
| account-service | `accountdb` | `accounts` (with `@Version` column) |
| transaction-service | `transactiondb` | `transactions`, `outbox_events` |
| fraud-service | `frauddb` | `fraud_evaluations` |
| notification-service | `notificationdb` | `notifications`, `processed_events` |
| ingestion-service | `ingestiondb` | `ingestion_logs`, `customer_segments` |

---

## 11. Kubernetes Design

**Cluster:** Self-managed on AWS EC2 using kubeadm
**Nodes:** 1 Master (t3.medium) + 1 Worker (t3.medium)

### K8s Resources per Service

Each microservice will have:
```yaml
# Deployment — runs N replicas
# Service — ClusterIP for internal, LoadBalancer for gateway
# ConfigMap — externalised config (DB URLs, Kafka brokers, topic names)
# Secret — DB passwords, JWT secret, OpenAI API key
# HorizontalPodAutoscaler — scale on CPU > 70%
```

### JVM Memory Cap (t3.medium optimisation)

Every service Dockerfile sets:
```dockerfile
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -Xmx256m"
```

With 7 services × 256MB max heap = ~1.8GB max — fits comfortably in t3.medium (4GB RAM) alongside Kubernetes system processes.

### Key K8s Concepts Practiced

| Concept | Where Used |
|---|---|
| `Deployment` + `ReplicaSet` | All services |
| `HorizontalPodAutoscaler (HPA)` | transaction-service, notification-service |
| `Liveness Probe` | All services — restart if unhealthy |
| `Readiness Probe` | All services — remove from load balancer if not ready |
| `ConfigMap` | DB URLs, Kafka config, topic names |
| `Secret` | DB passwords, JWT secret, OpenAI API key |
| `ClusterIP Service` | Internal service-to-service communication |
| `LoadBalancer Service` | api-gateway (external entry point) |
| `Namespace` | `payflow-dev` and `payflow-prod` |

### HPA Example (transaction-service)

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: transaction-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: transaction-service
  minReplicas: 1
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

---

## 12. CI/CD — Harness Pipeline

**Tool:** Harness Free Tier
**One pipeline per microservice**
**Trigger:** Path-based — pipeline for `user-service` only triggers when files under `user-service/` change

### Pipeline Stages (per service)

```
Stage 1: Build & Test (combined)
  └── mvn clean package -pl <service-name> -am
      (-am = also build modules this service depends on)
      (tests run as part of package phase — Testcontainers included)

Stage 2: Docker Build & Push
  └── docker build -t <ecr-url>/<service-name>:<git-sha> .
  └── docker push to AWS ECR

Stage 3: Deploy to K8s
  └── Harness K8s rolling deployment step
  └── Zero-downtime rolling update
  └── Auto-rollback on failed health check
```

### Path-Based Trigger Configuration (Harness)

```yaml
triggers:
  - name: user-service-trigger
    type: Webhook
    spec:
      pathFilters:
        - user-service/**
      branch: main

  - name: transaction-service-trigger
    type: Webhook
    spec:
      pathFilters:
        - transaction-service/**
      branch: main
```

> This means pushing a change to `fraud-service/` does not re-deploy `user-service`. Each service deploys independently.

### Branch Strategy

| Branch | Pipeline behaviour |
|---|---|
| `main` | Full pipeline: build + test + Docker + ECR + K8s deploy |
| `dev` | Build + test only (Stage 1), no Docker/deploy |
| Feature branch | No pipeline (local testing only) |

---

## 13. Testing Strategy

Testing is wired from **Week 1**, not added at the end. Every service gets its test scaffold when the service is first created.

| Type | Tool | What is Tested |
|---|---|---|
| Unit Tests | JUnit 5 + Mockito | Service layer logic, fraud rules, LLM response parsing, segmentation rules |
| Integration Tests | Testcontainers | Full flow with real PostgreSQL and real Kafka in Docker containers |
| API Tests | MockMvc / RestAssured | Controller layer, request validation, error responses |

### Key Test Scenarios

| Scenario | Service |
|---|---|
| Transaction approved when fraud score < 40 | transaction-service |
| no debit attempted (returns 403) | transaction-service |
| Optimistic lock conflict (409) on concurrent debit | account-service |
| Duplicate `idempotency-key` returns same response, no double charge | transaction-service |
| Duplicate `eventId` in notification-service is skipped | notification-service |
| SUCCESS notification uses template (no LLM call) | notification-service |
| FLAGGED notification triggers async LLM call | notification-service |
| Failed Kafka event retries 3× then lands in DLT | notification-service |
| Circuit breaker fires when OpenAI is down → rule-based fallback | fraud-service |
| Outbox poller publishes unpublished events after restart | transaction-service |
| Consent recorded and withdrawn correctly | user-service |
| Segmentation assigns HIGH_SPENDER correctly | ingestion-service |

---

## 14. Monitoring

**Tools:** Prometheus (metrics scraping) + Grafana (dashboards)

### Key Dashboards

| Dashboard | Metrics |
|---|---|
| Transaction Throughput | Transactions per second, success vs failure rate |
| Fraud Detection | Fraud flag rate, block rate, OpenAI API latency, fallback rate |
| Kafka Health | Consumer lag, messages per second, DLT event count, partition offsets |
| Notification Latency | P50/P95/P99 — template path vs LLM path separately |
| Service Latency | P50, P95, P99 response times per service |
| K8s Health | Pod count, CPU/memory usage, HPA scaling events |

Spring Boot Actuator exposes `/actuator/prometheus` on each service for Prometheus to scrape.

---

## 15. Project Structure (Maven Multi-Module)

```
payflow/
├── pom.xml                      # Parent POM — dependency management
├── api-gateway/
│   ├── pom.xml
│   └── src/
├── user-service/
│   ├── pom.xml
│   └── src/
├── account-service/
│   ├── pom.xml
│   └── src/
├── transaction-service/
│   ├── pom.xml
│   └── src/
├── fraud-service/
│   ├── pom.xml
│   └── src/
├── notification-service/
│   ├── pom.xml
│   └── src/
├── ingestion-service/
│   ├── pom.xml
│   └── src/
├── k8s/                         # All Kubernetes manifests
│   ├── api-gateway/
│   ├── user-service/
│   ├── transaction-service/
│   └── ...
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
├── docker-compose.yml           # Full stack (all services + infra)
└── docker-compose-infra.yml     # Infra only (DBs + Kafka) — use during development
```

---

## 15.1 Secrets Management & Spring Profiles

This section covers how configuration and secrets are handled across local development and production (K8s). It is pure DevOps interview material.

### Spring Profiles

Two active profiles: `dev` (local) and `prod` (K8s). Each service has:

```
src/main/resources/
├── application.yml          # shared config (port, JPA settings, Actuator)
├── application-dev.yml      # dev overrides (local DB URLs, relaxed settings)
└── application-prod.yml     # prod overrides (env-var-only secrets, strict settings)
```

**Set active profile via environment variable (never hardcode):**
```bash
# Locally (IntelliJ Run Config or terminal)
SPRING_PROFILES_ACTIVE=dev

# In Docker Compose
environment:
  SPRING_PROFILES_ACTIVE: dev

# In K8s Deployment manifest
env:
  - name: SPRING_PROFILES_ACTIVE
    value: "prod"
```

### Fail-Fast Secret Validation

In `application-prod.yml`, use the `${VAR:?error message}` syntax. Spring will **refuse to start** if the variable is missing — no silent fallback to a default in production:

```yaml
# application-prod.yml
jwt:
  secret: ${JWT_SECRET:?JWT_SECRET environment variable is required in prod}

openai:
  api:
    key: ${OPENAI_API_KEY:?OPENAI_API_KEY environment variable is required in prod}

spring:
  datasource:
    password: ${DB_PASSWORD:?DB_PASSWORD environment variable is required in prod}
```

In `application-dev.yml`, safe defaults are fine:
```yaml
# application-dev.yml
jwt:
  secret: dev-local-secret-not-for-production

spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/payflowdb?currentSchema=userdb
    username: payflow_user
    password: payflow123
```

### Local Secrets (.env file)

```bash
# .env (never commit to git — listed in .gitignore)
JWT_SECRET=payflow-super-secret-key-change-in-production-min-256-bits
OPENAI_API_KEY=sk-your-openai-key-here
DB_PASSWORD=payflow123
SPRING_PROFILES_ACTIVE=dev
```

Docker Compose reads `.env` automatically. For IntelliJ, add these to the Run Configuration's environment variables panel.

### K8s Secrets (Production)

Never put secrets in K8s ConfigMaps — ConfigMaps are plaintext. Use K8s Secrets:

```bash
# Create secret from .env file (run once, on the cluster)
kubectl create secret generic payflow-secrets \
  --from-env-file=.env \
  --namespace=payflow-prod
```

Reference secrets in Deployment manifests:
```yaml
env:
  - name: JWT_SECRET
    valueFrom:
      secretKeyRef:
        name: payflow-secrets
        key: JWT_SECRET

  - name: OPENAI_API_KEY
    valueFrom:
      secretKeyRef:
        name: payflow-secrets
        key: OPENAI_API_KEY

  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: payflow-secrets
        key: DB_PASSWORD
```

### Interview Talking Points for This Section

| Question | Answer |
|---|---|
| "How did you manage secrets across environments?" | `.env` locally, K8s Secrets in prod — never in code or ConfigMaps |
| "What's the difference between ConfigMap and Secret?" | ConfigMap is plaintext; Secret is base64-encoded and access-controlled by RBAC |
| "How did you prevent misconfigured prod deployments?" | `${VAR:?message}` syntax — Spring refuses to start if required env var is missing |
| "How did you handle different configs per environment?" | Spring Profiles (`dev`/`prod`) activated via `SPRING_PROFILES_ACTIVE` env var |

---

## 16. Phased Implementation Plan (5-Week Vertical Slice)

> Testing (JUnit + Testcontainers) and Harness CI are wired from Week 1. Each week delivers a working vertical slice — a feature that can be run and tested end to end.

| Week | Vertical Slice | What Gets Built |
|---|---|---|
| **Week 1** | **User + Account + Auth + Gateway** | user-service (register, login, JWT, consents), account-service (create, balance, debit with optimistic locking), api-gateway (JWT validation filter, routing to user-service and account-service). Flyway migrations. JUnit + Testcontainers scaffolded. Verify end-to-end: register → login → get JWT → call account API through gateway. Harness pipeline Stage 1 (build+test) wired for all three services. |
| **Week 2** | **Transaction Core + Fraud** | transaction-service (fraud context enrichment → fraud-service call → debit → outbox write). fraud-service (rule-based first, then OpenAI integration + Resilience4j circuit breaker). Integration tests for full transaction flow. Harness pipeline for both services. |
| **Week 3** | **Kafka + Notifications** | Outbox poller publishing to Kafka. notification-service (fast template for SUCCESS, async LLM for FLAGGED/BLOCKED). `@RetryableTopic` + DLT. `eventId` deduplication. Kafka UI verification. Harness pipeline for notification-service. |
| **Week 4** | **Ingestion + K8s Deploy** | ingestion-service (scheduled sync + loyalty/offer segmentation rules). K8s cluster setup on EC2 (kubeadm). K8s manifests for all services (Deployment, Service, ConfigMap, Secret, HPA, probes). Full Harness pipeline (build + test + Docker + ECR + K8s) for all services. |
| **Week 5** | **Monitoring + Hardening** | Prometheus + Grafana dashboards. JVM memory caps verified on K8s. Load test transaction-service to trigger HPA. Verify DLT, circuit breaker, and outbox recovery under failure. Polish test coverage. |

---

## 17. AWS Infrastructure

| Resource | Type | Purpose |
|---|---|---|
| EC2 Instance 1 | t3.medium | K8s Master Node |
| EC2 Instance 2 | t3.medium | K8s Worker Node (runs all service pods) |
| AWS ECR | Repository | Docker image registry |
| Security Groups | — | Control inbound/outbound traffic between nodes |

**Single PostgreSQL pod** serves all 6 schemas on the worker node. Each service connects to its own schema via separate credentials — logical isolation without the RAM cost of 6 separate Postgres instances.

**JVM Memory Budget on Worker Node (4GB RAM):**
```
7 services × 256MB max heap = ~1.8GB
PostgreSQL                  = ~512MB
Kafka (if on worker)        = ~512MB
K8s system processes        = ~512MB
─────────────────────────────────────
Total                       ≈ 3.3GB  ✓ fits in 4GB
```

**Cost Estimate (Monthly):**
| Resource | Estimated Cost |
|---|---|
| EC2 t3.medium x2 | ~₹1,200 |
| AWS ECR | ~₹50 |
| OpenAI API (low volume) | ~₹100–200 |
| **Total** | **~₹1,350–1,450/month** |

> **Cost tip:** Stop EC2 instances when not actively working. You only pay for uptime.

---

## 18. Inter-Service Communication Summary

| From | To | Protocol | When |
|---|---|---|---|
| API Gateway | All services | HTTP (REST) | Every client request |
| Transaction Service | Fraud Service | HTTP (REST, sync) | Step 2: fraud evaluation with enriched context (before any debit) |
| Transaction Service | Account Service | HTTP (REST, sync) | Step 3: balance debit — only if fraud decision is APPROVE or FLAG |
| Transaction Service | Outbox Table | DB write (atomic) | Step 4: same DB transaction as INSERT transactions |
| Outbox Poller | Kafka | Produce | Every 500ms: publishes unpublished outbox events |
| Notification Service | Kafka | Consume | Continuously via `@RetryableTopic` |
| Notification Service | OpenAI API | HTTPS (REST, async) | Only for FLAGGED/BLOCKED events |
| Fraud Service | OpenAI API | HTTPS (REST, sync) | Every fraud evaluation (with Resilience4j fallback) |
| Ingestion Service | Account Service | HTTP (REST) | Scheduled every 15 min |
| Ingestion Service | Transaction Service | HTTP (REST) | Scheduled every 15 min |

---

## 19. Interview Talking Points

| Interview Topic | PayFlow Coverage |
|---|---|
| "Tell me about your microservices experience" | 7 services, bounded contexts, independent schemas, API gateway |
| "How did you handle concurrent transactions?" | JPA `@Version` optimistic locking on account balance — prevents double-spend, returns 409 on conflict |
| "Why BigDecimal and not double for money?" | Floating point precision errors — `0.1 + 0.2 ≠ 0.3` in binary. BigDecimal is exact. NUMERIC(19,4) in Postgres. |
| "How did you ensure Kafka publish and DB write are atomic?" | Transactional Outbox pattern — both writes in one DB transaction, poller publishes separately |
| "How did you handle duplicate Kafka events?" | `eventId` deduplication — checked against `processed_events` table before processing |
| "What happens if Kafka consumer fails repeatedly?" | `@RetryableTopic` — 3 retries with exponential backoff, then routed to DLT for investigation |
| "Tell me about your Kafka experience" | Outbox → Kafka flow, partitioning, consumer groups, DLT, deduplication, sub-150ms SLA |
| "How did you handle fraud detection?" | LLM-based scoring with enriched context (30-day avg, 1-hour count) + Resilience4j circuit breaker fallback |
| "How did you meet sub-150ms notification latency?" | Fast templates for SUCCESS (<50ms), async LLM only for FLAGGED/BLOCKED (non-blocking) |
| "How did you ensure high availability?" | K8s liveness/readiness probes, HPA autoscaling, rolling deploys, outbox recovery |
| "What's your CI/CD experience?" | Harness path-based triggers, per-service pipelines, `mvn -pl -am` builds |
| "How did you test your services?" | JUnit + Mockito unit tests, Testcontainers integration tests, wired from Week 1 |
| "Have you worked with AI/LLM?" | OpenAI API for fraud scoring (sync with fallback) and personalised alerts (async, FLAGGED/BLOCKED only) |
| "What is GDPR compliance in your project?" | `user_consents` table — grant/withdraw consent endpoints, consent type tracking |
| "Tell me about loyalty/offers modules" | Rule-based segmentation in ingestion-service — HIGH_SPENDER, ELIGIBLE_OFFER, LOYALTY_TIER |
| "How do you monitor your services?" | Prometheus metrics, Grafana dashboards, separate panels for template vs LLM notification latency |
| "How did you optimise for limited cloud resources?" | Single Postgres instance with 6 schemas, JVM `-Xmx256m` cap per service, Harness path-based triggers |

---

*Document version: 1.3 | Project: PayFlow | Developer: [Your Name]*

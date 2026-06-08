# ✈️ SkyHub — Production-Grade Architecture & Engineering Blueprint

## Table of Contents

1. [Business Context & Design Trade-offs](#1-business-context--design-trade-offs)
2. [System Topology & Network Map](#2-system-topology--network-map)
3. [Inter-Service Communication Matrix](#3-inter-service-communication-matrix)
4. [Detailed Request & Response Flows](#4-detailed-request--response-flows)
5. [Microservice Definitions & Tech Stack](#5-microservice-definitions--tech-stack)
6. [JWT & Authentication Architecture](#6-jwt--authentication-architecture)
7. [Database Strategy & Connection Management](#7-database-strategy--connection-management)
8. [API Design Standards](#8-api-design-standards)
9. [Message Schema Design (Kafka & RabbitMQ)](#9-message-schema-design-kafka--rabbitmq)
10. [Security Architecture](#10-security-architecture)
11. [Observability: Logging, Metrics & Tracing](#11-observability-logging-metrics--tracing)
12. [Reliability Patterns](#12-reliability-patterns)
13. [Folder Structure](#13-folder-structure)
14. [Environment Configuration & Secrets](#14-environment-configuration--secrets)
15. [Infrastructure & Local Development](#15-infrastructure--local-development)
16. [Build Phases Roadmap](#16-build-phases-roadmap)

---

## 1. Business Context & Design Trade-offs

**SkyHub** is a production-ready, highly concurrent **Flight Search & Booking Platform** modeled after Expedia, Skyscanner, and MakeMyTrip. It demonstrates five core distributed systems trade-offs that every large-scale platform must resolve:

| # | The Problem | Our Decision | Why |
|---|---|---|---|
| 1 | **100×–1000× more reads than writes** — users search many times before one booking | CQRS: separate MongoDB read-model for search, PostgreSQL write-model for bookings | Search queries never block transactional writes |
| 2 | **Finite seat inventory** — two concurrent users must never book the same seat | Row-level `SELECT ... FOR UPDATE` PostgreSQL lock at hold-time + 15-minute hold expiry | ACID guarantee on the one resource that physically cannot be double-allocated |
| 3 | **Distributed transactions** across 3 separate databases | Saga Orchestration via RabbitMQ with SagaLog + compensating transactions | Two-Phase Commit across microservices is a latency and availability anti-pattern |
| 4 | **Personalized pricing without per-request User Service calls** | Stateless RS256 JWT claims propagated as HTTP headers by the Gateway | Zero network overhead for discount lookups during high-volume search |
| 5 | **Background work must not block API response threads** | BullMQ delayed job queues backed by Redis, consumed by isolated Notification worker | Client gets instant response; emails and reminders are processed asynchronously |

### Core User Journeys

Three primary flows drive every architectural decision in SkyHub:

| Journey | Services Touched (in order) | Communication Pattern |
|---------|-----------------------------|-----------------------|
| **Register / Login** | API Gateway → User Service → PostgreSQL | Sync HTTP + bcrypt + RS256 JWT |
| **Search Flights** | API Gateway → Search Service → Redis → MongoDB | Sync HTTP + Cache-Aside + CQRS read model |
| **Book a Flight** | API Gateway → Booking → Flight → Payment → Notification | Sync HTTP + Saga (RabbitMQ) + BullMQ async |

**Golden Rule for data ownership:** Every service owns exactly one database. No service ever queries another service's database directly. All cross-service data flows through HTTP, Kafka, or RabbitMQ.

---

### Synchronous Seat Hold vs. Event-Driven Asynchronous Hold

For Step 1 of the Booking Saga (holding seats), we explicitly chose a **Synchronous HTTP API Call** (`/internal/flights/:id/hold-seats`) rather than a fully asynchronous, event-driven pattern. 

#### Why Not a Purely Event-Driven (Async) Hold?
In a fully event-driven system:
1. The Booking Service publishes `BOOKING_INITIATED` to a message queue.
2. The Flight Service eventually consumes it, checks seat inventory, and publishes `SEAT_HELD` or `SEAT_HOLD_FAILED`.
3. The client browser has to wait on a loading spinner, polling the server or listening via WebSockets/SSE to see if the seat hold was successful.

While this decouples the services in time, it results in a **poor user experience** for high-contention resources like flight inventory. If a flight sells out, the user waits on a spinner only to get a "Reservation Failed" message minutes later.

#### Our Hybrid Approach
We combine synchronous inventory checks with asynchronous transaction finalization:
- **Synchronous hold-seats**: Returns a definitive success/fail immediately to the user during checkout initiation.
- **Asynchronous checkout & payment**: Handled via Saga Orchestration over RabbitMQ once the seat hold is verified.

#### Reliability Optimizations for Synchronous Coupling
To prevent the synchronous call from becoming a single point of failure or slowing down the system, we implement:
- **Circuit Breaker (`opossum`)**: Instantly fails-fast and shows a helpful error message if the Flight Service becomes unresponsive, rather than exhausting connection pools and hanging the Booking Service.
- **Internal HTTP Retries with Exponential Backoff (`axios-retry`)**: Automatically retries transient network blips (e.g., 1s, 2s, 4s delay) behind the scenes for maximum reliability.

### Service-Level Objectives (SLOs)

These targets shape every architectural decision. If a choice helps meet an SLO, it is the right choice.

| Endpoint | p99 Latency Target | Availability Target |
|---|---|---|
| `GET /api/v1/search` (cache hit) | < 150ms | 99.9% |
| `GET /api/v1/search` (cache miss) | < 800ms | 99.9% |
| `POST /api/v1/auth/login` | < 400ms | 99.95% |
| `POST /api/v1/bookings` | < 1.2s | 99.9% |
| `POST /api/v1/payments/process` | < 3s | 99.99% |
| Email/Notification delivery | < 30s (soft, async) | 99.5% |

---

## 2. System Topology & Network Map

### 2.1 Full Network Architecture

```text
═══════════════════════════════════════════════════════════════════════
  PUBLIC INTERNET ZONE
═══════════════════════════════════════════════════════════════════════

  [ Browser / Mobile App / Third-Party Client ]
                         │
              ( HTTPS / TLS 1.3 only — HTTP redirects to HTTPS )
                         │
          ┌──────────────▼──────────────┐
          │  CLOUDFLARE (Edge Layer)    │
          │  - WAF: blocks SQLi, XSS   │
          │  - DDoS mitigation          │
          │  - Rate limit at DNS edge   │
          └──────────────┬──────────────┘
                         │
          ┌──────────────▼──────────────┐
          │  LOAD BALANCER              │
          │  (NGINX / AWS ALB)          │
          │  - Round-robin distribution │
          │  - Health check probes      │
          └──────┬───────────────┬──────┘
                 │               │
═══════════════════════════════════════════════════════════════════════
  PRIVATE INTERNAL NETWORK (Docker Network / Kubernetes Cluster)
═══════════════════════════════════════════════════════════════════════
                 │               │
    ┌────────────▼───┐       ┌───▼────────────┐
    │ API GATEWAY    │       │ API GATEWAY    │
    │ Instance 1     │       │ Instance 2     │
    │ (Port 3000)    │       │ (Port 3000)    │
    └────────┬───────┘       └───────┬────────┘
             │  (Both share Redis for rate-limit counters & JWT blacklist)
             └──────────────┬────────┘
                            │
         ┌──────────────────┼──────────────────────┐
         │                  │                      │
         ▼                  ▼                      ▼
┌────────────────┐  ┌───────────────┐  ┌─────────────────────┐
│ USER SERVICE   │  │ FLIGHT SERVICE│  │  SEARCH SERVICE     │
│ Port: 3001     │  │ Port: 3002    │  │  Port: 3006         │
│ DB: user_db    │  │ DB: flight_db │  │  DB: search_db      │
│ (PostgreSQL)   │  │ (PostgreSQL)  │  │  (MongoDB)          │
│                │  │               │  │  Cache: Redis       │
└────────────────┘  └───────┬───────┘  └──────────▲──────────┘
                            │                     │
                            │  [Kafka Producer]   │ [Kafka Consumer]
                            └────── Kafka ────────┘
                               topic: flight-inventory-events
                               topic: user-identity-events

┌─────────────────────┐  [RabbitMQ: booking.initiated]  ┌────────────────────┐
│  BOOKING SERVICE    │ ──────────────────────────────► │  PAYMENT SERVICE   │
│  Port: 3003         │ ◄────────────────────────────── │  Port: 3004        │
│  DB: booking_db     │  [RabbitMQ: payment.result]     │  DB: payment_db    │
│  (PostgreSQL)       │                                 │  (PostgreSQL)      │
└─────────┬───────────┘                                 └──────────┬─────────┘
          │                                                        │
          │ [BullMQ: email-queue]                         (Stripe Webhook: HTTPS)
          │ [BullMQ: reminder-queue]                               │
          │ [BullMQ: seat-timeout-queue]                  [ stripe.com ] ──► POST /webhooks/stripe
          ▼
┌────────────────────────┐
│  NOTIFICATION SERVICE  │
│  (no inbound HTTP port)│
│  BullMQ Workers only   │
└────────────────────────┘

═══════════════════════════════════════════════════════════════════════
  SHARED INFRASTRUCTURE (Managed separately, not part of any service)
═══════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────┐
  │  Redis (Single logical instance, multiple logical databases) │
  │  DB 0: Rate-limit counters + JWT blacklist (Gateway)        │
  │  DB 1: Search result cache (Search Service)                 │
  │  DB 2: Idempotency keys (Payment Service)                   │
  │  DB 3: BullMQ job queues (Booking → Notification)           │
  └─────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────┐
  │  Apache Kafka (Topics)                                       │
  │  - flight-inventory-events  (Flight → Search)               │
  │  - user-identity-events     (User → Search)                 │
  └─────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────┐
  │  RabbitMQ (Exchanges + Queues)                               │
  │  - Exchange: skyhub.booking   Queue: booking.initiated      │
  │  - Exchange: skyhub.payment   Queue: payment.result         │
  │  - DLQ: booking.initiated.dlq / payment.result.dlq          │
  └─────────────────────────────────────────────────────────────┘
```

### 2.2 Why Two API Gateway Instances?

One instance is a **Single Point of Failure**. The Load Balancer detects which instance is healthy via `/health` probes and stops routing to a crashed instance within seconds. Both instances read/write the **same Redis** for rate-limit counters — this is critical. Without shared Redis, each instance has its own counter and users get 2× their rate limit.

### 2.3 Why Cloudflare / CDN at the Edge?

Your Node.js gateway can process ~5,000–15,000 requests/second before saturating. A basic DDoS attack sends 1,000,000 requests/second. Cloudflare absorbs and filters attack traffic at its global network edge **before it ever reaches your servers**. It also provides:
- Automatic TLS certificate management
- HTTP→HTTPS redirect enforcement
- Geographic IP blocking
- Web Application Firewall (WAF) rules for OWASP Top 10

### 2.4 Full Service Connection Map

This single diagram shows every service, its database, and every connection (HTTP, Kafka, RabbitMQ, BullMQ, Redis) at a glance. Read it top-down to trace any request through the system.

```text
                         CLIENT (Browser / Mobile)
                               │ HTTPS
                               ▼
                    Cloudflare → NGINX Load Balancer
                               │
                    ┌──────────▼──────────┐
                    │    API GATEWAY      │ :3000
                    │  Rate limit ────────┼──► Redis DB 0
                    │  JWT verify         │    (rate counters + JWT blacklist)
                    │  Header inject      │
                    │  Circuit breaker    │
                    └──┬───┬───┬───┬──────┘
                       │   │   │   │   (HTTP proxy — each arrow is a different route group)
          ┌────────────┘   │   │   └─────────────────────┐
          │                │   │                         │
          ▼                ▼   ▼                         ▼
  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  ┌──────────────┐
  │ USER SERVICE │  │FLIGHT SERVICE│  │  SEARCH  │  │   BOOKING    │
  │   :3001      │  │   :3002      │  │  SERVICE │  │   SERVICE    │
  │              │  │              │  │  :3006   │  │   :3003      │
  │ PostgreSQL   │  │ PostgreSQL   │  │          │  │              │
  │ skyhub_      │  │ skyhub_      │  │ MongoDB  │  │ PostgreSQL   │
  │ user_db      │  │ flight_db    │  │ Redis DB1│  │ skyhub_      │
  │              │  │              │  │          │  │ booking_db   │
  │ RS256 sign   │  │ FOR UPDATE   │  │          │  │ saga_logs    │
  │ bcrypt       │  │ row lock     │  │          │  │              │
  └──────┬───────┘  └──────┬───────┘  └────▲─────┘  └──┬─────┬────┘
         │                 │               │            │     │
         │ Kafka           │ Kafka         │ Kafka      │HTTP │RabbitMQ
         │ user-identity-  │ flight-       │ (consumer) │sync │booking.
         │ events          │ inventory-    │            │hold │initiated
         │                 │ events        │            │     │
         └────────────────►└───────────────┘            │     ▼
                     KAFKA BROKER                       │  ┌──────────────┐
                                                        │  │   PAYMENT    │
                                                        │  │   SERVICE    │
                                                        │  │   :3004      │
                                                        ▼  │              │
                                              FLIGHT SVC   │ PostgreSQL   │
                                              internal      │ skyhub_      │
                                              endpoints:    │ payment_db   │
                                              /hold-seats   │              │
                                              /release-seats│ Redis DB 2   │
                                                           │ (idempotency)│
                                                           │              │
                                                           │ Stripe SDK ──┼──► stripe.com
                                                           │ ▲            │    (external)
                                                           │ │ webhook     │
                                                           └──┬───────────┘
                                                              │ RabbitMQ
                                                              │ payment.result
                                                              ▼
                                                         BOOKING SERVICE
                                                         (RabbitMQ consumer)
                                                         Updates saga state
                                                              │
                                                              │ BullMQ
                                                              ▼ (Redis DB 3)
                                                    ┌─────────────────────┐
                                                    │  NOTIFICATION       │
                                                    │  SERVICE (worker)   │
                                                    │  No HTTP port       │
                                                    │                     │
                                                    │  Polls BullMQ jobs  │
                                                    │  → HTTP GET         │
                                                    │    /internal/       │
                                                    │    bookings/:id     │
                                                    │  → PDFKit           │
                                                    │  → SendGrid ────────┼──► user email
                                                    └─────────────────────┘
```

**How to read this diagram:**
- Solid arrows `──►` = always-on connections (every request)
- Every service box only talks to its own database (isolated ownership)
- The Kafka broker sits in the middle decoupling Flight writes from Search reads
- RabbitMQ sits between Booking and Payment for saga coordination
- Notification Service has no inbound traffic — it only consumes from BullMQ

---

## 3. Inter-Service Communication Matrix

```
┌──────────────────────┬──────────────────────┬───────────────┬──────────────────────────────────────────┐
│  FROM                │  TO                  │  PROTOCOL     │  WHY THIS PROTOCOL                       │
├──────────────────────┼──────────────────────┼───────────────┼──────────────────────────────────────────┤
│ Load Balancer        │ API Gateway (x2)     │ HTTP/HTTPS    │ Reverse proxy routing                    │
│ API Gateway          │ User Service         │ HTTP (proxy)  │ Synchronous: needs auth response now     │
│ API Gateway          │ Flight Service       │ HTTP (proxy)  │ Synchronous: admin CRUD                  │
│ API Gateway          │ Search Service       │ HTTP (proxy)  │ Synchronous: user needs search results   │
│ API Gateway          │ Booking Service      │ HTTP (proxy)  │ Synchronous: booking initiation          │
│ API Gateway          │ Payment Service      │ HTTP (proxy)  │ Synchronous: payment submission          │
│ API Gateway          │ Redis (DB 0)         │ Redis proto   │ Rate-limit counters + JWT blacklist      │
│ Booking Service      │ Flight Service       │ Sync HTTP     │ Seat hold — needs immediate ACID result  │
│ Flight Service       │ Kafka                │ Kafka proto   │ Async: FLIGHT_UPDATED event fan-out      │
│ User Service         │ Kafka                │ Kafka proto   │ Async: USER_REGISTERED/UPDATED event     │
│ Search Service       │ Kafka (consumer)     │ Kafka proto   │ Consume flight + user events             │
│ Booking Service      │ RabbitMQ             │ AMQP          │ Guaranteed saga command delivery         │
│ Payment Service      │ RabbitMQ             │ AMQP          │ Guaranteed saga result delivery          │
│ Booking Service      │ BullMQ (Redis DB 3)  │ Redis proto   │ Schedule email + reminder + timeout jobs │
│ Notification Service │ BullMQ (Redis DB 3)  │ Redis proto   │ Consume and process jobs                 │
│ Stripe               │ Payment Service      │ HTTPS webhook │ Stripe pushes payment result to us       │
└──────────────────────┴──────────────────────┴───────────────┴──────────────────────────────────────────┘
```

**Golden Rule:** Use synchronous HTTP only when the caller cannot proceed without an immediate answer. Use async messaging for everything else.

### 3.1 Communication Patterns — Channel Deep Dive

SkyHub uses four distinct channels. Each is chosen for a specific reason — not interchangeable.

#### Channel 1: Synchronous HTTP (User is Waiting)

```text
Client ──HTTPS──► Gateway ──HTTP──► Service
                                        │
                               1. Validate input (Zod)
                               2. DB query / cache hit
                               3. Business logic
                                        │
Client ◄──────────────────────── Response (sync, same request)
```

**Used for:** All client-facing endpoints, Booking → Flight seat hold.
**Why:** User is staring at a spinner. They need a definitive yes/no before they can proceed.
**Resilience stack:** Circuit breaker (opossum) fails fast → axios-retry handles transient blips.

---

#### Channel 2: Kafka (High-Throughput Event Streaming — CQRS)

```text
Flight Service                              Search Service
      │                                           │
      │ 1. Admin updates flight price             │
      │ 2. BEGIN TRANSACTION                      │
      │      INSERT flights ...                   │
      │      INSERT outbox_events ...  ← same tx  │
      │    COMMIT                                 │
      │                                           │
      │ 3. Outbox Worker polls (every 5s)         │
      ├──── Kafka: flight-inventory-events ──────►│
      │     Key = flightId                        │ 4. Upsert into MongoDB
      │     (same flight → same partition          │ 5. Invalidate Redis cache tags
      │      → ordered delivery guaranteed)       │    SMEMBERS tag:flight:{id} → DEL keys
```

**Used for:** Flight Service → Search Service (CQRS), User Service → consumers.
**Why:** Decouples the write-model from the read-model completely. Search handles 1000× more QPS than writes without competing for the same DB connections.
**Guarantee:** Outbox pattern → event is never lost even if the service crashes mid-publish.

---

#### Channel 3: RabbitMQ (Saga Coordination — Guaranteed Delivery)

```text
Booking Service                             Payment Service
      │                                           │
      │ 1. POST /bookings received                │
      │ 2. Sync hold-seats (HTTP) ✓               │
      │ 3. BEGIN TRANSACTION                      │
      │      INSERT bookings {status: PENDING}    │
      │      INSERT saga_logs {step: SEATS_HELD}  │
      │      INSERT outbox_events {BOOKING_INIT}  │
      │    COMMIT                                 │
      │ 4. Outbox Worker publishes ───────────────►│
      │    Exchange: skyhub.booking               │ 5. Check idempotency key (Redis DB 2)
      │    Queue: booking.initiated               │ 6. stripe.paymentIntents.create(...)
      │                                           │ 7. Stripe webhook arrives → INSERT payment
      │                                           │ 8. Outbox publishes result
      │◄─── Queue: payment.result ────────────────│
      │ 9. Update saga_logs                       │
      │    SUCCESS → UPDATE bookings CONFIRMED    │
      │    FAILED  → HTTP release-seats → ROLLBACK│
```

**Used for:** Booking ↔ Payment saga (the only place in the system with distributed state).
**Why:** RabbitMQ guarantees delivery even if one service is temporarily down. Messages wait in the queue — they are not lost.
**DLQ:** After 3 nack+requeue cycles → `booking.initiated.dlq` for manual ops inspection.

---

#### Channel 4: BullMQ (Background Jobs — Never Block the API Thread)

```text
Booking Service       Redis DB 3         Notification Worker
      │                    │                      │
      │ After CONFIRMED:   │                      │
      │ add job ───────────►                      │
      │  BOOKING_CONFIRM   │                      │
      │  PAYMENT_RECEIPT   │◄─── Worker polls ────│
      │  BOOKING_REMINDER  │                      │
      │    (delayed 24h)   │──── job payload ─────►
      │                              5. HTTP GET /internal/bookings/:id
      │                              6. PDFKit → generate ticket PDF
      │                              7. SendGrid → send email
      │                              Retry: 1min → 2min → 4min → DLQ
```

**Used for:** All notifications, flight reminders, seat-hold auto-expiry.
**Why:** PDF generation + email delivery takes 2–5 seconds. Blocking the API request thread for this would degrade all concurrent users. Client gets an instant `202 Accepted` and the work happens asynchronously.

---

## 4. Detailed Request & Response Flows

### 4.1 User Registration & Login

```text
CLIENT
  │
  └── POST /api/v1/auth/register ──────────────────── API GATEWAY (Port 3000)
                                                               │
                                               1. Check Redis rate-limit for client IP
                                                  (sliding-window: 20 req / 15min on auth routes)
                                               2. No JWT check needed (public route)
                                               3. Generate X-Correlation-ID
                                               4. Proxy to USER SERVICE
                                                               │
                                                               ▼
                                                    USER SERVICE (Port 3001)
                                               5.  Zod validates: name, email, password
                                                   password rules: min 8 chars, 1 uppercase,
                                                   1 digit, 1 special char
                                               6.  Check PostgreSQL: email already exists?
                                                   YES → throw 409 Conflict immediately
                                               7.  bcrypt.hash(password, 12)  [~200ms CPU]
                                               8.  BEGIN TRANSACTION
                                                     INSERT INTO users (...)
                                                     INSERT INTO outbox_events (type='USER_REGISTERED', payload=...)
                                                   COMMIT  ← both writes in one ACID transaction
                                               9.  Return 201 Created { userId, name, email, role, loyaltyTier }
                                                               │
                                            (Background: Outbox Worker)
                                               10. Reads outbox_events table
                                               11. Publishes USER_REGISTERED → Kafka: user-identity-events
                                               12. Marks outbox event as published

✅ Client sees: 201 Created with user profile

─────────── LOGIN FLOW ───────────

CLIENT
  └── POST /api/v1/auth/login ─────────────────────── API GATEWAY → USER SERVICE
                                               1.  Zod validates email + password
                                               2.  Fetch user by email (B-Tree indexed — sub-ms)
                                               3.  Check user.is_active = true, email_verified = true
                                               4.  bcrypt.compare(password, hash) [~200ms]
                                                   FAIL → increment failed_login_attempts
                                                         if attempts >= 5: set locked_until = NOW() + 30min
                                                         throw 401 Unauthorized
                                               5.  Reset failed_login_attempts = 0
                                               6.  Sign RS256 ACCESS TOKEN (15 min):
                                                   payload: { sub: userId, role, loyaltyTier, jti: uuid() }
                                               7.  Generate REFRESH TOKEN: crypto.randomBytes(64).toString('hex')
                                               8.  Store SHA-256 hash of refresh token in refresh_tokens table
                                               9.  Return 200 OK { accessToken, refreshToken, user: {...} }
                                                               │
                                            (Background)
                                               10. Update users.last_login_at = NOW()

✅ Client sees: tokens + user profile. Client stores:
   - accessToken in memory (never localStorage — XSS risk)
   - refreshToken in HttpOnly cookie or secure storage
```

### 4.2 Flight Search (CQRS Read Path)

```text
CLIENT
  └── GET /api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY
                                               │
                                     API GATEWAY (Port 3000)
                                               │
                                 1. Parse JWT from Authorization header (optional)
                                    If present: verify RS256 signature + check jti not in Redis blacklist
                                    Extract loyaltyTier claim → default to 'SILVER' if no JWT
                                 2. Inject headers:
                                    X-Correlation-ID: <uuid>
                                    X-User-Loyalty-Tier: GOLD        ← from verified JWT
                                    X-User-Id: <userId>              ← from verified JWT (if present)
                                 3. Proxy to SEARCH SERVICE
                                               │
                                               ▼
                                    SEARCH SERVICE (Port 3006)
                                 4. Build cache key:
                                    key = "search:DEL:BOM:2026-10-12:2:ECONOMY"
                                    (origin:dest:date:passengers:cabin — all dimensions included)
                                               │
                                 5. Redis GET key  (DB 1)
                                    ├── HIT  → deserialize JSON, jump to step 8
                                    └── MISS → query MongoDB:
                                               db.flights.find({
                                                 origin: "DEL", destination: "BOM",
                                                 departureDate: "2026-10-12",
                                                 availableSeats: { $gte: 2 },
                                                 cabinClass: "ECONOMY"
                                               }).hint({ origin:1, destination:1, departureDate:1 })
                                 6. Cache result: Redis SET key <json> EX 300  (5 min TTL)
                                 7. Tag the cache entry: Redis SADD "tag:flight:{flightId}" key
                                    (so we can invalidate by flightId later — O(1) not O(N))
                                 8. Apply loyalty discount to each flight price:
                                    SILVER → 5% | GOLD → 10% | PLATINUM → 15%
                                 9. Filter: availableSeats < passengers → remove from results
                                10. Return paginated result set

✅ Client sees: personalised flight list with discounted prices
```

### 4.3 Seat Cache Invalidation (CQRS Write Path)

```text
ADMIN CLIENT
  └── POST /api/v1/flights ──────────────────── API GATEWAY
                                 1. Validate JWT: role must be FLIGHT_ADMIN or SUPER_ADMIN
                                    If role check fails → 403 Forbidden
                                 2. Proxy to FLIGHT SERVICE
                                               │
                                               ▼
                                    FLIGHT SERVICE (Port 3002)
                                 3. Zod validates payload
                                 4. BEGIN TRANSACTION
                                      INSERT INTO flights (...)
                                      INSERT INTO outbox_events (type='FLIGHT_UPDATED', payload=...)
                                    COMMIT
                                 5. Return 201 Created to admin

(Background: Outbox Worker)
                                 6. Reads outbox table, publishes:
                                    FLIGHT_UPDATED → Kafka topic: flight-inventory-events
                                 7. Marks event as published

(Kafka Consumer: Search Service — background)
                                 8. Consume FLIGHT_UPDATED event
                                 9. Upsert flight document into MongoDB search_db
                                10. Invalidate stale cache:
                                    affected_keys = Redis SMEMBERS "tag:flight:{flightId}"
                                    Redis DEL ...affected_keys   ← O(1) tag lookup, O(M) delete
                                    Redis DEL "tag:flight:{flightId}"
                                    (NO KEYS command — non-blocking tag-based invalidation)

✅ Admin sees: 201 Created instantly
✅ Search results: updated within milliseconds (eventual consistency)
```

### 4.4 Booking Checkout — Full Saga Orchestration

**The Booking Service is the Saga Orchestrator.** It drives every step, tracks state in a `saga_logs` table, and decides what happens next. This is Orchestration Saga (not choreography).

#### Step A: Booking Initiation

```text
CLIENT
  └── POST /api/v1/bookings { flightId, seats: 2 } ──── API GATEWAY
                                 1. Verify JWT, extract userId
                                 2. Inject X-User-Id + X-Correlation-ID
                                 3. Proxy to BOOKING SERVICE
                                               │
                                               ▼
                                    BOOKING SERVICE (Port 3003)
                                 4. Zod validates payload

                                 ─── SAGA STEP 1: HOLD SEATS ───
                                 5. Sync HTTP PATCH → Flight Service: /internal/flights/:id/hold-seats
                                    Flight Service:
                                      BEGIN TRANSACTION
                                        SELECT * FROM flights WHERE id = ? FOR UPDATE  ← row lock
                                        IF availableSeats < 2 → ROLLBACK → return 400
                                        UPDATE flights SET availableSeats = availableSeats - 2
                                        INSERT INTO outbox_events (type='SEATS_HELD', ...)
                                      COMMIT
                                      Return { success: true, heldUntil: NOW() + 15min }
                                 6. If 400 (no seats) → return 409 Conflict to client immediately

                                 ─── SAGA STEP 2: CREATE BOOKING RECORD ───
                                 7. BEGIN TRANSACTION (Booking DB)
                                      INSERT INTO bookings (status='PENDING_PAYMENT', heldUntil=...)
                                      INSERT INTO saga_logs (bookingId, state='SEAT_HELD')
                                      INSERT INTO outbox_events (type='BOOKING_INITIATED', payload={bookingId, totalPrice, ...})
                                    COMMIT  ← all three writes atomically
                                    (Outbox pattern: event publication is guaranteed even if service crashes)

                                 ─── SAGA STEP 3: SCHEDULE SEAT HOLD EXPIRY ───
                                 8. BullMQ: add delayed job to 'seat-timeout-queue'
                                    delay = 15 minutes
                                    data = { bookingId, flightId, seats: 2 }
                                    jobId = bookingId  ← deterministic ID prevents duplicate jobs

                                 9. Return 201 { bookingId, totalPrice, expiresAt } to client

✅ Client sees: booking created, redirected to payment page with 15-min countdown
```

#### Step B: Payment Processing (Stripe Webhook Flow)

```text
CLIENT
  └── POST /api/v1/payments/initiate { bookingId, amount }
                                               │
                                    PAYMENT SERVICE (Port 3004)
                                 1. Validate Idempotency-Key header (required)
                                 2. Check Redis DB 2: GET "idem:{idempotencyKey}"
                                    HIT  → return cached response immediately (no double processing)
                                    MISS → proceed
                                 3. Create Stripe PaymentIntent:
                                    stripe.paymentIntents.create({ amount, currency: 'inr', metadata: { bookingId } })
                                    Returns: { clientSecret, paymentIntentId }
                                 4. Save payment record: status='PENDING', paymentIntentId
                                 5. Cache: Redis SET "idem:{idempotencyKey}" {paymentIntentId} EX 2592000
                                    (TTL = 30 days — matching Stripe's own idempotency window)
                                 6. Return { clientSecret } to client

CLIENT (browser)
  └── Uses Stripe.js + clientSecret to show card UI, user enters card details
      Stripe.js confirms payment directly with Stripe servers
      (card details NEVER touch your servers — PCI DSS compliance)

STRIPE (external)
  └── POST /api/v1/webhooks/stripe ──────────────── PAYMENT SERVICE
                                 7. Verify Stripe-Signature header using webhook secret
                                    INVALID signature → 400, log, discard
                                 8. Parse event type:
                                    'payment_intent.succeeded'  → publish PAYMENT_SUCCESS to RabbitMQ
                                    'payment_intent.failed'     → publish PAYMENT_FAILED to RabbitMQ
                                 9. Update payment record status in PostgreSQL
                                10. Return 200 OK to Stripe immediately
                                    (Stripe retries if it gets non-2xx within 30 seconds)

✅ Client sees: Stripe UI handles payment — no card data in your system
```

#### Step C: Saga Resolution — Happy Path

```text
RabbitMQ [skyhub.payment → payment.result: PAYMENT_SUCCESS]
  └── CONSUMED BY ─────────────────────────────── BOOKING SERVICE
                                 1. Check: is booking already CONFIRMED?
                                    YES → ack message and return (consumer idempotency guard)
                                    NO  → proceed
                                 2. BEGIN TRANSACTION
                                      UPDATE bookings SET status='CONFIRMED'
                                      UPDATE saga_logs SET state='COMPLETED'
                                    COMMIT
                                 3. Cancel the seat-hold expiry BullMQ job (it's no longer needed)
                                    bullmq.remove(jobId = bookingId)
                                 4. BullMQ: add job to 'email-queue'
                                    data = { bookingId }  ← store only the ID, not PII
                                 5. Calculate: reminderFireAt = departureTime - 24 hours
                                    BullMQ: add delayed job to 'reminder-queue'
                                    delay = reminderFireAt - NOW()
                                    data = { bookingId }

BullMQ [email-queue]
  └── CONSUMED BY ─────────────────────────────── NOTIFICATION SERVICE
                                 6. Fetch booking details from Booking DB (via internal API)
                                    (Job only stores bookingId, not PII in Redis)
                                 7. Generate PDF ticket (PDFKit — lightweight, no headless browser)
                                 8. Send email via SendGrid API (not raw SMTP)
                                    On SendGrid failure → BullMQ auto-retries:
                                    attempt 1: 1 min | attempt 2: 5 min | attempt 3: 30 min
                                    After 3 failures → job moves to 'email-queue-failed' DLQ
                                    Ops team can inspect and replay from DLQ

✅ User receives: booking confirmation email with PDF ticket
```

#### Step D: Saga Rollback — Payment Failed

```text
RabbitMQ [payment.result: PAYMENT_FAILED]
  └── CONSUMED BY ─────────────────────────────── BOOKING SERVICE
                                 1. Check: is booking already CANCELLED?
                                    YES → ack and return (idempotency guard)
                                    NO  → proceed
                                 2. BEGIN TRANSACTION
                                      UPDATE bookings SET status='CANCELLED'
                                      UPDATE saga_logs SET state='ROLLBACK_INITIATED'
                                      INSERT INTO outbox_events (type='RELEASE_SEATS', ...)
                                    COMMIT

(Outbox Worker)
                                 3. Publish RELEASE_SEATS command via HTTP with retry:
                                    PATCH /internal/flights/:id/release-seats
                                    If Flight Service down → retry with exponential backoff
                                    Max 5 retries over 10 minutes
                                    If all retries fail → alert ops team via DLQ
                                 4. On success: UPDATE saga_logs SET state='ROLLBACK_COMPLETED'

✅ Seats publicly available again. No payment taken.
```

#### Step E: Seat Hold Expiry (User Abandons Payment)

```text
BullMQ [seat-timeout-queue] fires after 15 minutes
  └── CONSUMED BY ─────────────────────────────── BOOKING SERVICE (worker)
                                 1. Fetch booking by bookingId
                                 2. Is status still 'PENDING_PAYMENT'?
                                    NO  → booking was confirmed or cancelled — skip (idempotency)
                                    YES → proceed
                                 3. UPDATE bookings SET status='TIMED_OUT'
                                    UPDATE saga_logs SET state='TIMED_OUT'
                                 4. HTTP PATCH → Flight Service: /release-seats
                                 5. (Optional) BullMQ: add job → 'email-queue' with type='BOOKING_EXPIRED'
                                    Notification Service sends "Your booking expired" email

✅ Seats returned to inventory automatically. No manual intervention needed.
```

### 4.5 SagaLog State Machine

```text
                       ┌─────────────┐
                       │   STARTED   │ ← booking initiated, seat hold API called
                       └──────┬──────┘
              ┌───────────────┼────────────────┐
              ▼               ▼                ▼
    ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐
    │ SEAT_HOLD_   │  │  SEAT_HELD  │  │  SEAT_HOLD_FAILED│ ← flight sold out
    │ FAILED       │  └──────┬──────┘  └──────────────────┘
    └──────────────┘         │
                    ┌────────┴───────────┐
                    ▼                   ▼
           ┌──────────────┐    ┌─────────────────┐
           │  TIMED_OUT   │    │ PAYMENT_SUCCESS  │
           └──────────────┘    └────────┬────────┘
                                        ▼
                               ┌─────────────────┐
                               │   COMPLETED     │ ← final happy state
                               └─────────────────┘

                    ┌─────────────────────┐
                    │  PAYMENT_FAILED     │
                    └─────────┬───────────┘
                              ▼
                    ┌─────────────────────┐
                    │  ROLLBACK_INITIATED │
                    └─────────┬───────────┘
               ┌──────────────┴──────────────┐
               ▼                             ▼
    ┌────────────────────┐         ┌─────────────────────┐
    │  ROLLBACK_COMPLETED│         │   ROLLBACK_FAILED   │ ← Flight Service unreachable
    └────────────────────┘         └─────────────────────┘
                                          (ops alert)
```

---

## 5. Microservice Definitions & Tech Stack

### Standardized Tech Stack (All Services)

> **ORM Decision:** All services use **Prisma** (not Sequelize). Prisma generates a fully type-safe client from your schema, catches type errors at compile time, and has better TypeScript support. Mixing Sequelize and Prisma across services doubles cognitive overhead with no benefit.

> **Logger:** All services use **Pino** (not Winston). Pino is 5× faster than Winston, outputs structured JSON natively, and integrates directly with async context for automatic correlation ID injection.

> **JWT Algorithm:** All services that verify JWTs use **RS256** (not HS256). See Section 6 for full explanation.

---

### 1. API Gateway (Port 3000)

**Purpose:** The single public door. Handles auth, routing, rate limiting, and header injection. Never contains business logic.

| Concern | Library | Why |
|---|---|---|
| HTTP Server | Express + TypeScript | Familiar, lightweight |
| Reverse Proxy | `http-proxy-middleware` | Sufficient for monorepo dev |
| JWT Verification | `jose` (not `jsonwebtoken`) | RS256 / JWKS support, actively maintained |
| Rate Limiting | `express-rate-limit` + `rate-limit-redis` | Redis store ensures distributed rate limits work across both gateway instances |
| Circuit Breaker | `opossum` | Opens after N consecutive failures — stops cascading collapse |
| Redis Client | `ioredis` | Blacklist checks + rate limit store |
| Correlation ID | `uuid` | Generates X-Correlation-ID per request |
| Logging | `pino` + `pino-http` | Structured JSON logs with auto request/response timing |

**Responsibilities:**
- **TLS Termination:** Accept HTTPS from Load Balancer; forward HTTP internally.
- **JWT Validation:** Verify RS256 signature using User Service's public key (fetched once via JWKS endpoint, cached in memory). Check `jti` claim against Redis blacklist.
- **Distributed Rate Limiting:** Sliding-window 100 req/15min per IP globally. Stricter limit on auth routes: 20 req/15min per IP.
- **Header Injection:** Forward verified claims as trusted internal headers:
  - `X-User-Id: <userId>`
  - `X-User-Role: CUSTOMER | FLIGHT_ADMIN | SUPER_ADMIN`
  - `X-User-Loyalty-Tier: SILVER | GOLD | PLATINUM`
  - `X-User-Jti: <jti>` — JWT ID, forwarded so User Service can write it to the Redis blacklist on logout
  - `X-Correlation-ID: <uuid>`
- **Circuit Breaker:** Per-upstream breaker. If User Service returns 5xx 3 times in a row, open the circuit for 30s — return 503 immediately without attempting the call.
- **API Versioning:** All routes are prefixed `/api/v1/`. Future breaking changes go to `/api/v2/` without disrupting existing clients.

---

### 2. User Service (Port 3001)

**Purpose:** Authoritative identity store. Only service that touches `user_db` or stores password hashes.

| Concern | Library | Why |
|---|---|---|
| HTTP Server | Express + TypeScript | Standard |
| Database | PostgreSQL via Prisma | ACID for user identity |
| Validation | Zod | Type-safe schema validation |
| Password Hashing | `bcrypt` (cost 12) | Adaptive, GPU-resistant |
| JWT Signing | `jose` with RS256 private key | Private key never leaves this service |
| Redis Client | `ioredis` | JWT blacklist write |
| Kafka Producer | `kafkajs` | Publish user identity events |
| Logging | `pino` | Structured JSON |

**Responsibilities:**
- Registration, login, token refresh, logout.
- Publish `USER_REGISTERED` and `USER_LOYALTY_UPDATED` events to Kafka.
- Expose `GET /.well-known/jwks.json` endpoint — the public key used to verify tokens.
- Outbox table pattern ensures Kafka events are never lost even on crash.

---

### 3. Flight Service (Port 3002)

**Purpose:** Write-side owner of the flight catalog and seat inventory. The only service that modifies flight data.

| Concern | Library | Why |
|---|---|---|
| HTTP Server | Express + TypeScript | Standard |
| Database | PostgreSQL via Prisma | ACID for seat inventory |
| Validation | Zod | |
| Kafka Producer | `kafkajs` | Publish FLIGHT_UPDATED / SEATS_UPDATED events |
| Logging | `pino` | |

**Responsibilities:**
- Admin endpoints: create/update/delete flights (RBAC: FLIGHT_ADMIN, SUPER_ADMIN only).
- Internal endpoints (not proxied by Gateway): `/internal/flights/:id/hold-seats` and `/internal/flights/:id/release-seats` — called only by Booking Service.
- Publishes all mutations to Kafka via Outbox pattern.

---

### 4. Search Service (Port 3006)

**Purpose:** Read-optimized CQRS read model. High-throughput, stateless, never writes to Flight DB.

| Concern | Library | Why |
|---|---|---|
| HTTP Server | Express + TypeScript | Standard |
| Database | MongoDB via Mongoose | Flexible schema, compound index support for complex filter queries |
| Cache | Redis `ioredis` (DB 1) | Cache-aside, 5-minute TTL |
| Kafka Consumer | `kafkajs` (consumer group: `search-service-group`) | Consume flight + user identity events |
| Logging | `pino` | |

**Responsibilities:**
- Serve `GET /api/v1/search` queries with cache-aside Redis strategy.
- Maintain local MongoDB read model — updated exclusively via Kafka events.
- Apply loyalty tier discounts from `X-User-Loyalty-Tier` header in-memory.
- Cache invalidation via tag-based Redis sets (never `KEYS *` pattern).

**MongoDB Indexes (Required — defined in schema):**
```js
// Compound index for the primary search query
{ origin: 1, destination: 1, departureDate: 1, cabinClass: 1 }

// Filter for available seats
{ availableSeats: 1 }

// Kafka upsert lookups
{ flightId: 1 }  // unique index

// Price range filter
{ basePrice: 1 }
```

---

### 5. Booking Service (Port 3003)

**Purpose:** Saga Orchestrator for the checkout transaction. Owns the booking lifecycle from PENDING to CONFIRMED or CANCELLED.

| Concern | Library | Why |
|---|---|---|
| HTTP Server | Express + TypeScript | |
| Database | PostgreSQL via Prisma | Financial bookings need ACID |
| RabbitMQ Client | `amqplib` | Publish BOOKING_INITIATED, consume PAYMENT_RESULT |
| BullMQ Client | `bullmq` | Schedule email, reminder, and seat-timeout jobs |
| HTTP Client | `axios` with retry | Call Flight Service hold/release endpoints |
| Circuit Breaker | `opossum` | Protect against Flight Service being down |
| Logging | `pino` | |

**Responsibilities:**
- Initiate booking, coordinate saga steps, handle all rollback paths.
- **Synchronous Seat Hold & locking**: Connect synchronously to the Flight Service's seat-hold endpoint using a row-level pessimistic lock (`SELECT ... FOR UPDATE`) in `flight_db` to guarantee atomic seat reservation.
- **Resilience Engineering**: Protect internal HTTP calls to the Flight Service with a circuit breaker (`opossum`) and transient network retries with exponential backoff (`axios-retry`).
- SagaLog tracks every state transition for debugging and recovery.
- All RabbitMQ consumers are idempotent — check current state before acting.
- Outbox pattern for guaranteed RabbitMQ event publishing.
- BullMQ `seat-timeout-queue` job auto-releases seats on payment abandonment.

---

### 6. Payment Service (Port 3004)

**Purpose:** Transaction ledger. Processes payments via Stripe, enforces idempotency, publishes saga results.

| Concern | Library | Why |
|---|---|---|
| HTTP Server | Express + TypeScript | |
| Database | PostgreSQL via Prisma | Payment ledger must be ACID |
| RabbitMQ Client | `amqplib` | Publish PAYMENT_SUCCESS / PAYMENT_FAILED |
| Stripe SDK | `stripe` | Official Node.js SDK |
| Redis | `ioredis` (DB 2) | Idempotency key store (30-day TTL) |
| Logging | `pino` | |

**Responsibilities:**
- Create Stripe PaymentIntents, return `clientSecret` to client.
- Receive and validate Stripe webhooks (`payment_intent.succeeded`, `payment_intent.failed`).
- Idempotency engine prevents double-charges.
- All monetary amounts stored and transmitted in **minor units** (paise for INR, cents for USD). Example: ₹999.00 = `99900`. Never use floats for money.
- 30-day idempotency key retention in Redis.

---

### 7. Notification Service (No inbound port)

**Purpose:** Pure async background worker. Sends emails and generates PDF tickets. No HTTP server.

| Concern | Library | Why |
|---|---|---|
| Job Engine | `bullmq` Worker | Consume from email-queue and reminder-queue |
| PDF Generation | `PDFKit` | Lightweight (no headless browser), programmatic control |
| Email Provider | SendGrid SDK | Production deliverability, tracking, bounce handling |
| Logging | `pino` | |

**Responsibilities:**
- Consume `email-queue` jobs: fetch booking details via internal HTTP → generate PDF → send via SendGrid.
- Consume `reminder-queue` jobs: send check-in reminder email 24h before departure.
- Consume `seat-timeout-queue` jobs: (delegated to Booking Service worker — not Notification Service).
- All job data stores only `bookingId` (not PII). Service fetches details on demand.
- Failed jobs after 3 retries → move to Dead Letter Queue (`email-queue-failed`) for ops inspection.

---

## 6. JWT & Authentication Architecture

### Why RS256, Not HS256

| Property | HS256 (Symmetric) | RS256 (Asymmetric) |
|---|---|---|
| Signing | Shared secret | Private key (User Service only) |
| Verification | Same shared secret | Public key (any service can have it) |
| Attack surface | Every service that verifies tokens must hold the secret — if Booking Service is compromised, attacker can forge tokens for the whole cluster | Only User Service holds the private key — a compromised Booking Service cannot forge tokens |
| Key distribution | Risky | Safe — public key is public by definition |

### Implementation

**User Service** generates an RSA key pair on startup (or loads from env):
```text
PRIVATE KEY → used only inside User Service to sign JWTs
PUBLIC KEY  → exposed at GET /.well-known/jwks.json (JWKS endpoint)
```

**API Gateway** fetches the JWKS on startup, caches in memory, rotates every 24h:
```text
GET http://user-service:3001/.well-known/jwks.json
→ { keys: [{ kty, n, e, kid, alg }] }
```

### JWT Payload Design

```json
{
  "sub": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
  "role": "CUSTOMER",
  "loyaltyTier": "GOLD",
  "jti": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "iat": 1782500000,
  "exp": 1782500900
}
```

**What is NOT in the payload:**
- `email` — PII, not needed by downstream services
- `name` — not needed
- `password` — obviously never
- Any sensitive data — the payload is Base64-encoded, not encrypted

**`jti` (JWT ID):** A UUID per token. On logout, `jti` is stored in Redis:
```
Redis SET "blacklist:jti:{jti}" 1 EX {remaining_ttl_seconds}
```
Gateway checks `EXISTS blacklist:jti:{jti}` on every request — O(1) lookup.

### Refresh Token Security

The refresh token sent to the client is a 64-char cryptographically random hex string. What is stored in the database is:
```
stored_value = SHA-256(raw_token)
```
On `/api/v1/auth/refresh`:
```
incoming_hash = SHA-256(token_from_request)
lookup: SELECT * FROM refresh_tokens WHERE token_hash = incoming_hash
```
This way, if the `refresh_tokens` table is leaked, attackers get only hashes — useless without the original tokens.

### Refresh Token Rotation

On every successful `/api/v1/auth/refresh` call:
1. Delete the old refresh token row from the DB.
2. Generate a new refresh token.
3. Insert new token row with fresh 7-day expiry.
4. Return new `accessToken` + new `refreshToken`.

If an old refresh token is used after rotation, it is not found in the DB → `401 Unauthorized`. This detects refresh token theft.

---

## 7. Database Strategy & Connection Management

### Database Isolation

Each service owns exactly one database. No other service may query it directly.

| Service | Database Name | Engine | ORM |
|---|---|---|---|
| User Service | `skyhub_user_db` | PostgreSQL | Prisma |
| Flight Service | `skyhub_flight_db` | PostgreSQL | Prisma |
| Booking Service | `skyhub_booking_db` | PostgreSQL | Prisma |
| Payment Service | `skyhub_payment_db` | PostgreSQL | Prisma |
| Search Service | `skyhub_search_db` | MongoDB | Mongoose |

### Connection Pooling (Critical)

PostgreSQL's default `max_connections = 100`. Without pooling, 4 services × 10 Prisma pool connections = 40 connections used just for normal operation. Under load, this exhausts the limit and every new query throws `too many connections`.

Prisma's built-in pool is configured per service:
```
DATABASE_URL="postgresql://user:pass@host:5432/db?connection_limit=10&pool_timeout=10"
```

For production at scale, add **PgBouncer** as a connection pooler between services and PostgreSQL, allowing thousands of app connections to share a small pool of actual DB connections.

### Redis Logical Database Allocation

A single Redis instance serves four completely separate purposes via logical database numbers. Each service connects to its own DB — no data mixing, no accidental cross-service reads.

| DB | Owner | What's Stored | Key Pattern | TTL |
|----|-------|--------------|-------------|-----|
| **DB 0** | API Gateway | Rate-limit counters per IP | `rl:{ip}` | 15 min sliding window |
| **DB 0** | API Gateway | JWT blacklist (on logout) | `blacklist:jti:{jti}` | Token's remaining lifetime |
| **DB 0** | API Gateway | JWKS public key cache | `jwks:user-service` | 1 hour |
| **DB 1** | Search Service | Search result cache | `search:{origin}:{dest}:{date}:{pax}:{cabin}` | 5 min |
| **DB 1** | Search Service | Cache invalidation tag sets | `tag:flight:{flightId}` | 5 min |
| **DB 2** | Payment Service | Idempotency keys | `idem:{bookingId}` | 30 days |
| **DB 3** | Booking Service | BullMQ job store (producer) | Internal BullMQ keys | Per-job config |
| **DB 3** | Notification Service | BullMQ job store (worker) | Internal BullMQ keys | Per-job config |

**Why separate logical DBs instead of just key prefixes?**

| Reason | Explanation |
|--------|-------------|
| Accidental wipe safety | `FLUSHDB` on DB 1 (stale search cache) cannot touch DB 0 (JWT blacklist) |
| Different eviction policies | Cache (DB 1) can use `allkeys-lru`; blacklist (DB 0) must use `noeviction` |
| Cleaner mental model | Each service's Redis URL in `.env` points to its own DB number — ownership is explicit |
| Operational isolation | Redis `INFO keyspace` shows per-DB stats; easy to see if the cache is bloated |

**Tag-based cache invalidation (DB 1):**
```text
When admin updates flight FL001:
  Search Service Kafka consumer runs:
    keys = Redis SMEMBERS "tag:flight:FL001"   → ["search:DEL:BOM:...", "search:DEL:HYD:..."]
    Redis DEL keys[0], keys[1], ...            → removes all cached searches containing FL001
    Redis DEL "tag:flight:FL001"               → clean up the tag set itself

Why tags instead of KEYS *:
  KEYS * is O(N) and blocks Redis — dangerous at scale.
  Tag sets are O(1) lookup + O(M) delete where M = number of affected cache entries only.
```

---

### Transactional Outbox Pattern

Every service that needs to publish a message to Kafka or RabbitMQ uses the Outbox pattern:

```text
❌ Naive approach (loses events on crash):
  1. Write to DB
  2. Publish to Kafka   ← crash here = DB updated but Kafka never gets the event

✅ Outbox pattern (guaranteed delivery):
  1. BEGIN TRANSACTION
       Write business data (e.g., new user)
       Write to outbox_events table (pending, same DB)
     COMMIT   ← atomic: both succeed or both fail
  2. Background Outbox Worker polls outbox_events
  3. Publishes event to Kafka/RabbitMQ
  4. Marks outbox_event as 'published'
```

The `outbox_events` table (one per service):
```sql
CREATE TABLE outbox_events (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type  VARCHAR(100)   NOT NULL,
  payload     JSONB          NOT NULL,
  status      VARCHAR(20)    NOT NULL DEFAULT 'PENDING',  -- PENDING | PUBLISHED | FAILED
  created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox_events(status, created_at) WHERE status = 'PENDING';
```

### Migration Strategy

All Prisma schema changes use tracked migrations:
```bash
npx prisma migrate dev --name add_email_verified_column    # dev
npx prisma migrate deploy                                   # production (no data loss)
```

Migrations are committed to git alongside code. Never use `prisma db push` in production (no migration history).

---

## 8. API Design Standards

### URL Convention

```
/api/v1/{resource}/{id}/{sub-resource}

Examples:
POST   /api/v1/auth/register
POST   /api/v1/auth/login
POST   /api/v1/auth/refresh
POST   /api/v1/auth/logout
POST   /api/v1/auth/logout-all
GET    /api/v1/auth/me
PUT    /api/v1/auth/me

GET    /api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY
GET    /api/v1/flights/:id
POST   /api/v1/flights                        ← FLIGHT_ADMIN only
PATCH  /api/v1/flights/:id                    ← FLIGHT_ADMIN only

POST   /api/v1/bookings
GET    /api/v1/bookings/:id
GET    /api/v1/bookings                        ← list own bookings
DELETE /api/v1/bookings/:id                   ← cancel booking

POST   /api/v1/payments/initiate
POST   /api/v1/webhooks/stripe                ← Stripe webhook (not proxied, direct)

GET    /health                                 ← health check (all services, no /api/v1 prefix)
GET    /metrics                                ← Prometheus scrape endpoint (all services)
GET    /.well-known/jwks.json                  ← User Service only
```

### Standardized Response Envelope

**Success:**
```json
{
  "success": true,
  "message": "Human-readable description",
  "data": { },
  "meta": {
    "page": 1, "limit": 20, "total": 150
  },
  "traceId": "tr-f47ac10b-58cc-4372-a567"
}
```

**Error:**
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Human-readable error",
    "details": [
      { "field": "password", "message": "Password must be at least 8 characters" }
    ]
  },
  "traceId": "tr-f47ac10b-58cc-4372-a567"
}
```

**Standard Error Codes:**
| HTTP Status | `error.code` | When to use |
|---|---|---|
| 400 | `VALIDATION_ERROR` | Zod schema failed |
| 401 | `UNAUTHORIZED` | Missing or invalid JWT |
| 401 | `TOKEN_EXPIRED` | JWT expired |
| 401 | `TOKEN_BLACKLISTED` | Logged-out JWT reused |
| 403 | `FORBIDDEN` | Valid JWT but wrong role |
| 404 | `NOT_FOUND` | Resource does not exist |
| 409 | `CONFLICT` | Duplicate email, double booking |
| 422 | `BUSINESS_RULE_VIOLATION` | No seats available |
| 429 | `RATE_LIMIT_EXCEEDED` | Too many requests |
| 500 | `INTERNAL_ERROR` | Unexpected server error |
| 503 | `SERVICE_UNAVAILABLE` | Circuit breaker open |

---

## 9. Message Schema Design (Kafka & RabbitMQ)

All messages use a standard envelope with versioning. Schema changes that break consumers require a version bump, not in-place modification.

### Standard Message Envelope

```json
{
  "eventId":       "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "eventType":     "FLIGHT_UPDATED",
  "eventVersion":  "1.0",
  "source":        "flight-service",
  "correlationId": "req-abc123",
  "timestamp":     "2026-05-28T10:00:00.000Z",
  "payload":       { }
}
```

### Kafka Topics & Event Payloads

**Topic: `flight-inventory-events`**
```json
// FLIGHT_UPDATED
{ "flightId": "...", "origin": "DEL", "destination": "BOM",
  "departureDate": "2026-10-12", "basePrice": 499900, "availableSeats": 142,
  "cabinClass": "ECONOMY", "airline": "IndiGo" }

// SEATS_HELD
{ "flightId": "...", "seatsHeld": 2, "remainingSeats": 140, "heldUntil": "..." }

// SEATS_RELEASED
{ "flightId": "...", "seatsReleased": 2, "remainingSeats": 142 }
```

**Topic: `user-identity-events`**
```json
// USER_REGISTERED
{ "userId": "...", "loyaltyTier": "SILVER", "role": "CUSTOMER" }

// USER_LOYALTY_UPDATED
{ "userId": "...", "previousTier": "SILVER", "newTier": "GOLD" }
```

Note: Email is intentionally excluded — the Search Service needs only `userId` and `loyaltyTier`.

### RabbitMQ Exchange + Queue Design

```
Exchange: skyhub.booking  (type: direct)
  Routing key: booking.initiated → Queue: booking.initiated
                                    DLQ: booking.initiated.dlq (after 3 nack + requeue cycles)

Exchange: skyhub.payment  (type: direct)
  Routing key: payment.result   → Queue: payment.result
                                    DLQ: payment.result.dlq
```

**`BOOKING_INITIATED` payload:**
```json
{
  "bookingId": "...", "userId": "...", "flightId": "...",
  "seats": 2, "totalAmount": 99980, "currency": "INR",
  "correlationId": "..."
}
```
`totalAmount` is in **paise** (minor units). ₹999.80 = `99980`. Never float.

---

## 10. Security Architecture

### 10.1 Request Security Headers (Helmet.js — All Services)

Every Express service registers `helmet()` which sets:
```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Strict-Transport-Security: max-age=31536000; includeSubDomains
Content-Security-Policy: default-src 'none'
```

### 10.2 CORS Policy (API Gateway Only)

Internal services are not exposed to browsers. Only the Gateway needs CORS:
```typescript
app.use(cors({
  origin: process.env.ALLOWED_ORIGINS?.split(',') ?? ['http://localhost:5173'],
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Idempotency-Key'],
  credentials: true,     // required for HttpOnly cookie support
}));
```

### 10.3 Secrets Management

**Never hardcode secrets. Never commit `.env` files.**

| Environment | How Secrets Are Stored |
|---|---|
| Local Development | `.env` file (gitignored), `.env.example` committed with placeholder values |
| Docker Compose | `environment:` section referencing host env vars |
| Kubernetes (Prod) | Kubernetes Secrets → injected as env vars into pods |
| Cloud (Advanced) | AWS Secrets Manager / HashiCorp Vault → sidecar fetches on boot |

### 10.4 Internal Service Authentication

In local dev: internal services trust headers from the Gateway (X-User-Id, X-User-Role). No additional auth between services.

In production (Kubernetes): use **mTLS** enforced by a service mesh (Istio / Linkerd). Every service-to-service call has a mutual TLS certificate. A compromised pod cannot impersonate another service.

### 10.5 Database Security

- ORM (Prisma) uses parameterized queries by default — SQL injection is not possible through normal query methods.
- Never concatenate user input into raw SQL strings.
- Database users have **least-privilege**: user_service_user can only access `skyhub_user_db`. It cannot touch flight_db or booking_db.

### 10.6 Account Security (User Service)

| Threat | Defense |
|---|---|
| Brute-force login | Lock account for 30min after 5 failed attempts (`failed_login_attempts`, `locked_until` columns) |
| Credential stuffing | Redis rate limit: max 20 auth requests / 15min per IP on auth routes |
| Password exposure | bcrypt(cost=12) — 200ms per hash, GPU-infeasible |
| Token theft | 15-min access token TTL + JWT blacklist on logout |
| Refresh token theft | SHA-256 hashed storage + rotation on every use |
| Unverified email | `email_verified = false` gates login until email verification link clicked |

### 10.7 Stripe Webhook Security

```typescript
const sig = req.headers['stripe-signature'];
const event = stripe.webhooks.constructEvent(req.rawBody, sig, process.env.STRIPE_WEBHOOK_SECRET);
// Throws if signature invalid — prevents forged webhook calls
```

The endpoint uses `express.raw()` middleware (not `express.json()`) for the webhook route — Stripe's signature is computed against the raw bytes.

---

## 11. Observability: Logging, Metrics & Tracing

A system you cannot observe is a system you cannot debug. All three pillars (logs, metrics, traces) are required.

### 11.1 Structured Logging (Pino)

**Why Pino over Winston:** Pino writes JSON 5× faster than Winston by deferring serialization. Under load, slow loggers become a bottleneck.

Every log line automatically includes:
```json
{
  "level": "info",
  "time": "2026-05-28T10:00:00.000Z",
  "service": "user-service",
  "correlationId": "req-f47ac10b",
  "userId": "7b58c281",
  "msg": "User login successful",
  "responseTime": 312,
  "statusCode": 200
}
```

**Implementation:** Use `AsyncLocalStorage` to carry `correlationId` across async operations without passing it explicitly to every function:
```typescript
// Set in middleware once per request
asyncLocalStorage.run({ correlationId, userId }, () => next());

// Auto-injected in logger by reading from storage
logger.info('User registered');  // correlationId is added automatically
```

**Log Levels:**
- `error` — unexpected server errors, unhandled exceptions
- `warn` — business rule violations (wrong password, rate limit hit)
- `info` — successful operations (login, booking confirmed)
- `debug` — detailed flow (only in dev, never in production)

### 11.2 Metrics (Prometheus + prom-client)

Every service exposes `GET /metrics` in Prometheus text format. A Prometheus server scrapes this endpoint every 15 seconds.

**Metrics every service exposes:**
```typescript
// HTTP request rate + latency histogram
const httpRequestDuration = new Histogram({
  name: 'http_request_duration_ms',
  help: 'Duration of HTTP requests in ms',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [10, 50, 100, 200, 500, 1000, 3000],
});

// Unhandled error counter
const errorCounter = new Counter({
  name: 'http_errors_total',
  help: 'Total number of HTTP errors',
  labelNames: ['route', 'status_code'],
});
```

**Service-specific metrics:**
- Booking Service: `saga_state_transitions_total{state}`, `active_pending_bookings`
- Search Service: `cache_hit_total`, `cache_miss_total`, `search_query_duration_ms`
- Notification Service: `emails_sent_total`, `emails_failed_total`, `dlq_depth`

**Grafana dashboards:** Visualize all metrics. Alert rules fire PagerDuty/Slack when:
- Error rate > 1% for 5 minutes
- p99 latency > 2× SLO target
- DLQ depth > 0 for 10 minutes

### 11.3 Distributed Tracing (OpenTelemetry)

The `X-Correlation-ID` header is how you manually correlate logs. OpenTelemetry provides **automatic distributed traces** — a visual waterfall showing exactly how long each service, DB query, and cache call takes for a single user request.

```typescript
// Each service initialises OpenTelemetry on startup
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';

const sdk = new NodeSDK({
  traceExporter: new OTLPTraceExporter({ url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT }),
  instrumentations: [getNodeAutoInstrumentations()],  // auto-instruments Express, Prisma, ioredis
});
```

Traces are exported to **Jaeger** (local dev) or **Grafana Tempo** (production). Every DB query, Redis call, and HTTP request becomes a span in the trace.

### 11.4 Health Check Endpoints (All Services)

```typescript
// GET /health — used by Load Balancer + Kubernetes probes
app.get('/health', async (req, res) => {
  const checks = {
    database: await checkDatabaseConnection(),
    redis:    await checkRedisConnection(),     // (services that use Redis)
    kafka:    await checkKafkaConnection(),     // (services that use Kafka)
  };
  const healthy = Object.values(checks).every(v => v === 'ok');
  res.status(healthy ? 200 : 503).json({ status: healthy ? 'healthy' : 'degraded', checks });
});
```

Kubernetes uses two types:
- **Liveness probe:** Is the process alive? (`/health`) — if fails, container is restarted.
- **Readiness probe:** Is the service ready for traffic? (`/health`) — if fails, traffic is removed from rotation until it recovers.

---

## 12. Reliability Patterns

### 12.1 Graceful Shutdown

When a container is killed (`SIGTERM` from Kubernetes during deployment), in-flight requests must complete and connections must close cleanly. Without this, active requests get `ECONNRESET` errors on every deployment.

```typescript
// server.ts — every service
const server = app.listen(PORT);

process.on('SIGTERM', async () => {
  logger.info('SIGTERM received — beginning graceful shutdown');

  server.close(() => {                    // stop accepting new connections
    logger.info('HTTP server closed');
  });

  await prisma.$disconnect();            // close DB pool cleanly
  await redisClient.quit();             // close Redis connections
  await kafkaProducer.disconnect();     // flush pending Kafka messages
  await rabbitmqConnection.close();     // ack pending RabbitMQ messages

  logger.info('Graceful shutdown complete');
  process.exit(0);
});
```

### 12.2 Circuit Breaker (API Gateway + Booking Service)

Prevents cascade failure. Without a circuit breaker, if Flight Service is slow (200ms → 5s), every booking request ties up a thread for 5 seconds. With 100 concurrent users, the Booking Service runs out of threads and becomes unresponsive — even though its own code is fine.

```typescript
import CircuitBreaker from 'opossum';

const flightServiceBreaker = new CircuitBreaker(callFlightService, {
  timeout: 3000,               // fail if call takes > 3s
  errorThresholdPercentage: 50, // open after 50% failures
  resetTimeout: 30000,          // try again after 30s
});

flightServiceBreaker.fallback(() => ({
  success: false,
  error: { code: 'SERVICE_UNAVAILABLE', message: 'Flight service is temporarily unavailable' }
}));
```

States: `CLOSED` (normal) → `OPEN` (failing fast) → `HALF_OPEN` (testing recovery) → `CLOSED`.

### 12.3 Internal HTTP Retry (Booking → Flight)

Transient network blips should not fail user requests:

```typescript
import axiosRetry from 'axios-retry';

axiosRetry(axiosInstance, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,   // 1s, 2s, 4s
  retryCondition: (error) =>
    axiosRetry.isNetworkOrIdempotentRequestError(error) ||
    error.response?.status === 503,
});
```

Only retry idempotent operations. `hold-seats` is NOT idempotent (retrying it could hold 2× seats). Use the circuit breaker for hold-seats, not retries.

### 12.4 RabbitMQ Consumer Idempotency

All RabbitMQ consumers check current state before acting:

```typescript
async function handlePaymentSuccess(event: PaymentSuccessEvent) {
  const booking = await prisma.booking.findUnique({ where: { id: event.bookingId } });

  if (!booking || booking.status === 'CONFIRMED') {
    logger.info({ bookingId: event.bookingId }, 'Already confirmed — skipping duplicate message');
    return;  // ack the message without reprocessing
  }

  // ... process the event
}
```

### 12.5 BullMQ Dead Letter Queue

```typescript
const emailQueue = new Queue('email-queue', { connection: redis });
const emailWorker = new Worker('email-queue', processEmail, {
  connection: redis,
  attempts: 3,
  backoff: { type: 'exponential', delay: 60000 },  // 1min, 2min, 4min
});

emailWorker.on('failed', (job, error) => {
  if (job?.attemptsMade >= 3) {
    dlqQueue.add('failed-email', { originalJob: job?.data, error: error.message });
    logger.error({ jobId: job?.id }, 'Email job moved to DLQ after 3 failures');
  }
});
```

### 12.6 Outbox Worker Reliability

The Outbox Worker is a background cron that runs every 5 seconds inside each service:

```typescript
setInterval(async () => {
  const pendingEvents = await prisma.outboxEvent.findMany({
    where: { status: 'PENDING' },
    orderBy: { createdAt: 'asc' },
    take: 100,
  });

  for (const event of pendingEvents) {
    try {
      await publishToKafkaOrRabbitMQ(event);
      await prisma.outboxEvent.update({
        where: { id: event.id },
        data: { status: 'PUBLISHED', publishedAt: new Date() },
      });
    } catch (error) {
      logger.error({ eventId: event.id }, 'Outbox publish failed — will retry');
      // Next interval will retry
    }
  }
}, 5000);
```

---

## 13. Folder Structure

> **Naming convention:** All service directories use the `-service` suffix. The `services/` directory contains runnable services. The `packages/` directory contains shared libraries.

```text
SkyHub/                              ← Root Monorepo Directory
│
├── services/                        ← Runnable microservices (each is an independent Node.js process)
│   │
│   ├── api-gateway/                 ← Phase 1: Public entry point + reverse proxy
│   │   ├── src/
│   │   │   ├── config/
│   │   │   │   ├── env.ts           ← Zod-validated env vars (fails fast if misconfigured)
│   │   │   │   └── redis.config.ts
│   │   │   ├── middlewares/
│   │   │   │   ├── auth.middleware.ts     ← RS256 JWT verify + jti blacklist check
│   │   │   │   ├── rateLimit.middleware.ts← Redis sliding-window rate limiter
│   │   │   │   ├── cors.middleware.ts
│   │   │   │   ├── circuitBreaker.ts     ← opossum breakers per upstream
│   │   │   │   └── error.middleware.ts
│   │   │   ├── routes/
│   │   │   │   └── proxy.routes.ts       ← Maps /api/v1/auth/* → user-service, etc.
│   │   │   ├── app.ts
│   │   │   └── server.ts
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── user-service/                ← Phase 1: Identity, auth, loyalty
│   │   ├── src/
│   │   │   ├── config/
│   │   │   │   ├── env.ts
│   │   │   │   ├── database.ts      ← Prisma client singleton
│   │   │   │   ├── redis.config.ts
│   │   │   │   └── kafka.config.ts
│   │   │   ├── models/              ← Prisma schema lives in prisma/schema.prisma
│   │   │   ├── repositories/
│   │   │   │   ├── user.repository.ts
│   │   │   │   └── token.repository.ts
│   │   │   ├── services/
│   │   │   │   ├── auth.service.ts        ← registration, login, logout logic
│   │   │   │   ├── token.service.ts       ← JWT sign/verify, refresh token management
│   │   │   │   └── loyalty.service.ts     ← tier upgrade rules
│   │   │   ├── controllers/
│   │   │   │   └── auth.controller.ts
│   │   │   ├── routes/
│   │   │   │   └── auth.routes.ts
│   │   │   ├── middlewares/
│   │   │   │   ├── validate.ts
│   │   │   │   └── error.ts
│   │   │   ├── events/
│   │   │   │   ├── producers/
│   │   │   │   │   └── user.producer.ts   ← Kafka producer
│   │   │   │   └── outbox.worker.ts       ← Polls outbox_events, publishes to Kafka
│   │   │   ├── types/
│   │   │   │   └── express.d.ts           ← Augment req with userId, role, correlationId
│   │   │   ├── app.ts
│   │   │   └── server.ts
│   │   ├── prisma/
│   │   │   ├── schema.prisma
│   │   │   └── migrations/
│   │   ├── scripts/
│   │   │   └── seed.ts                    ← Seeds SUPER_ADMIN, FLIGHT_ADMIN from env vars
│   │   ├── tests/
│   │   │   ├── unit/
│   │   │   └── integration/
│   │   ├── .env.example
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── flight-service/              ← Phase 2: Flight catalog + seat inventory write side
│   │   ├── src/
│   │   │   ├── config/
│   │   │   ├── repositories/
│   │   │   ├── services/
│   │   │   ├── controllers/
│   │   │   ├── routes/
│   │   │   │   ├── admin.routes.ts         ← /api/v1/flights (FLIGHT_ADMIN only)
│   │   │   │   └── internal.routes.ts      ← /internal/flights/:id/hold-seats (no Gateway)
│   │   │   ├── middlewares/
│   │   │   ├── events/
│   │   │   │   └── outbox.worker.ts
│   │   │   ├── prisma/
│   │   │   ├── tests/
│   │   │   ├── app.ts
│   │   │   └── server.ts
│   │   ├── .env.example
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── search-service/              ← Phase 3: CQRS read model + personalized pricing
│   │   ├── src/
│   │   │   ├── config/
│   │   │   ├── models/
│   │   │   │   └── flight.model.ts         ← Mongoose schema with compound indexes
│   │   │   ├── repositories/
│   │   │   ├── services/
│   │   │   │   ├── search.service.ts
│   │   │   │   └── cache.service.ts        ← Tag-based Redis cache operations
│   │   │   ├── controllers/
│   │   │   ├── routes/
│   │   │   ├── events/
│   │   │   │   └── consumers/
│   │   │   │       ├── flight.consumer.ts  ← Kafka: flight-inventory-events
│   │   │   │       └── user.consumer.ts    ← Kafka: user-identity-events
│   │   │   ├── app.ts
│   │   │   └── server.ts
│   │   ├── tests/
│   │   ├── .env.example
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── booking-service/             ← Phase 4: Saga orchestrator + checkout
│   │   ├── src/
│   │   │   ├── config/
│   │   │   ├── repositories/
│   │   │   │   ├── booking.repository.ts
│   │   │   │   └── sagaLog.repository.ts
│   │   │   ├── services/
│   │   │   │   ├── booking.service.ts
│   │   │   │   └── saga.service.ts
│   │   │   ├── controllers/
│   │   │   ├── routes/
│   │   │   ├── events/
│   │   │   │   ├── producers/
│   │   │   │   │   └── booking.producer.ts  ← RabbitMQ: BOOKING_INITIATED
│   │   │   │   ├── consumers/
│   │   │   │   │   └── payment.consumer.ts  ← RabbitMQ: PAYMENT_RESULT
│   │   │   │   └── outbox.worker.ts
│   │   │   ├── workers/
│   │   │   │   └── seatTimeout.worker.ts    ← BullMQ worker: seat-timeout-queue
│   │   │   ├── prisma/
│   │   │   ├── tests/
│   │   │   ├── app.ts
│   │   │   └── server.ts
│   │   ├── .env.example
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── payment-service/             ← Phase 5: Stripe integration + idempotency ledger
│   │   ├── src/
│   │   │   ├── config/
│   │   │   ├── repositories/
│   │   │   ├── services/
│   │   │   │   ├── payment.service.ts
│   │   │   │   └── idempotency.service.ts   ← Redis-backed idempotency engine
│   │   │   ├── controllers/
│   │   │   │   ├── payment.controller.ts
│   │   │   │   └── webhook.controller.ts    ← Stripe webhook handler
│   │   │   ├── routes/
│   │   │   ├── events/
│   │   │   │   ├── producers/
│   │   │   │   │   └── payment.producer.ts  ← RabbitMQ: PAYMENT_RESULT
│   │   │   │   ├── consumers/
│   │   │   │   │   └── booking.consumer.ts  ← RabbitMQ: BOOKING_INITIATED
│   │   │   │   └── outbox.worker.ts
│   │   │   ├── prisma/
│   │   │   ├── tests/
│   │   │   ├── app.ts
│   │   │   └── server.ts
│   │   ├── .env.example
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   └── notification-service/        ← Phase 6: Background email + PDF worker (no HTTP server)
│       ├── src/
│       │   ├── config/
│       │   │   ├── env.ts
│       │   │   └── redis.config.ts
│       │   ├── workers/
│       │   │   ├── email.worker.ts          ← BullMQ: email-queue
│       │   │   └── reminder.worker.ts       ← BullMQ: reminder-queue
│       │   ├── services/
│       │   │   ├── pdf.service.ts           ← PDFKit ticket generation
│       │   │   └── email.service.ts         ← SendGrid API wrapper
│       │   └── server.ts                    ← No HTTP server — only starts BullMQ workers
│       ├── tests/
│       ├── .env.example
│       ├── package.json
│       └── tsconfig.json
│
├── packages/                        ← Shared libraries (zero business logic, pure utilities)
│   │
│   ├── shared-types/                ← Domain TypeScript enums, interfaces, Zod schemas
│   │   ├── src/
│   │   │   ├── enums/
│   │   │   │   ├── UserRole.ts          ← CUSTOMER | FLIGHT_ADMIN | SUPER_ADMIN
│   │   │   │   ├── LoyaltyTier.ts       ← SILVER | GOLD | PLATINUM
│   │   │   │   ├── BookingStatus.ts     ← PENDING_PAYMENT | CONFIRMED | CANCELLED | TIMED_OUT
│   │   │   │   └── SagaState.ts         ← Full state machine enum
│   │   │   ├── events/
│   │   │   │   ├── FlightEvents.ts      ← FLIGHT_UPDATED, SEATS_HELD interfaces
│   │   │   │   ├── UserEvents.ts        ← USER_REGISTERED, USER_LOYALTY_UPDATED
│   │   │   │   └── BookingEvents.ts     ← BOOKING_INITIATED, PAYMENT_RESULT
│   │   │   └── index.ts
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── common-utils/                ← Shared runtime utilities
│   │   ├── src/
│   │   │   ├── logger.ts            ← Pino factory with AsyncLocalStorage correlation
│   │   │   ├── AppError.ts          ← Typed error class with error.code + HTTP status
│   │   │   ├── asyncContext.ts      ← AsyncLocalStorage for correlationId propagation
│   │   │   ├── validateEnv.ts       ← Zod-based env validator (call on startup)
│   │   │   └── index.ts
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   └── message-broker/              ← Thin wrappers around Kafka, RabbitMQ, BullMQ clients
│       ├── src/
│       │   ├── kafka/
│       │   │   ├── producer.ts      ← KafkaJS producer with retry + standard envelope
│       │   │   └── consumer.ts      ← KafkaJS consumer factory
│       │   ├── rabbitmq/
│       │   │   ├── publisher.ts     ← amqplib publisher with exchange setup
│       │   │   └── consumer.ts      ← amqplib consumer with DLQ wiring
│       │   ├── bullmq/
│       │   │   └── queues.ts        ← Named queue + worker factory
│       │   └── index.ts
│       ├── package.json
│       └── tsconfig.json
│
├── docker-compose.yml               ← Boots all infrastructure (NOT the services themselves)
├── .env.example                     ← Root-level example showing all required env vars
├── tsconfig.base.json
├── tsconfig.json
├── turbo.json
├── eslint.config.js
├── .prettierrc
├── .prettierignore
└── package.json
```

### Package Dependency Graph

Shared packages are built **before** services. Changing any package triggers a rebuild in every dependent service — Turbo tracks this automatically via its dependency graph in `turbo.json`.

```text
                    ┌─────────────────────────────────────────────┐
                    │             SHARED PACKAGES                  │
                    │  (built first, zero business logic)          │
                    ├──────────────┬──────────────┬───────────────┤
                    │ common-utils │ shared-types │message-broker │
                    │              │              │               │
                    │ AppError     │ ErrorCode    │ Kafka wrapper │
                    │ Pino logger  │ BookingStatus│ RabbitMQ wrap │
                    │ validateEnv  │ LoyaltyTier  │ BullMQ factory│
                    │ AsyncStorage │ Event ifaces │               │
                    └──────┬───────┴──────┬───────┴───────┬───────┘
                           │              │               │
                           └──────────────▼───────────────┘
                                          │  workspace:* imports
                     ┌────────────────────┼────────────────────┐
                     │                    │                    │
              ┌──────▼──────┐     ┌───────▼──────┐    ┌───────▼──────┐
              │ user-service│     │flight-service│    │search-service│
              └─────────────┘     └──────────────┘    └──────────────┘
              ┌──────▼──────┐     ┌───────▼──────┐    ┌───────▼──────┐
              │booking-svc  │     │payment-svc   │    │notification  │
              └─────────────┘     └──────────────┘    │   -service   │
                                                       └──────────────┘
                                    api-gateway
```

**What each package provides to services:**

| Package | Import | What services get |
|---------|--------|-------------------|
| `@skyhub/common-utils` | `workspace:*` | `AppError` (typed errors with HTTP status), `logger` (Pino with auto correlation ID), `validateEnv` (Zod env check on startup) |
| `@skyhub/shared-types` | `workspace:*` | `ErrorCode` enum, `BookingStatus`, `LoyaltyTier`, `SagaState`, Kafka/RabbitMQ event TypeScript interfaces |
| `@skyhub/message-broker` | `workspace:*` | Pre-configured Kafka producer/consumer factory, RabbitMQ publisher/consumer with DLQ wiring, BullMQ named queue factory |

**Turbo build order (from `turbo.json`):**
```text
Step 1 (parallel):  build common-utils, build shared-types, build message-broker
Step 2 (parallel):  build all 7 services  ← unblocked once Step 1 finishes
Step 3 (parallel):  lint + test all packages and services

Cache hit: if a package's source files haven't changed, Turbo skips its rebuild
           and reuses the cached output — subsequent builds are near-instant.
```

---

## 14. Environment Configuration & Secrets

### `.env.example` (Root Reference — each service has its own)

```bash
# ── API Gateway ─────────────────────────────────────────────────────
GATEWAY_PORT=3000
USER_SERVICE_URL=http://user-service:3001
FLIGHT_SERVICE_URL=http://flight-service:3002
SEARCH_SERVICE_URL=http://search-service:3006
BOOKING_SERVICE_URL=http://booking-service:3003
PAYMENT_SERVICE_URL=http://payment-service:3004
REDIS_URL=redis://redis:6379/0
ALLOWED_ORIGINS=http://localhost:5173,https://skyhub.com
RATE_LIMIT_WINDOW_MS=900000
RATE_LIMIT_MAX_REQUESTS=100
JWKS_URI=http://user-service:3001/.well-known/jwks.json

# ── User Service ─────────────────────────────────────────────────────
PORT=3001
DATABASE_URL=postgresql://user:pass@postgres:5432/skyhub_user_db?connection_limit=10
REDIS_URL=redis://redis:6379/0
KAFKA_BROKERS=kafka:9092
JWT_PRIVATE_KEY_PATH=./keys/private.pem
JWT_PUBLIC_KEY_PATH=./keys/public.pem
JWT_ACCESS_TOKEN_EXPIRY=15m
JWT_REFRESH_TOKEN_EXPIRY_DAYS=7
BCRYPT_ROUNDS=12
SUPER_ADMIN_EMAIL=admin@skyhub.com
SUPER_ADMIN_PASSWORD=<from-secrets-manager>

# ── Flight Service ───────────────────────────────────────────────────
PORT=3002
DATABASE_URL=postgresql://user:pass@postgres:5432/skyhub_flight_db?connection_limit=10
KAFKA_BROKERS=kafka:9092

# ── Search Service ───────────────────────────────────────────────────
PORT=3006
MONGODB_URI=mongodb://mongo:27017/skyhub_search_db
REDIS_URL=redis://redis:6379/1
KAFKA_BROKERS=kafka:9092
KAFKA_GROUP_ID=search-service-group

# ── Booking Service ──────────────────────────────────────────────────
PORT=3003
DATABASE_URL=postgresql://user:pass@postgres:5432/skyhub_booking_db?connection_limit=10
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672
REDIS_URL=redis://redis:6379/3
FLIGHT_SERVICE_INTERNAL_URL=http://flight-service:3002

# ── Payment Service ──────────────────────────────────────────────────
PORT=3004
DATABASE_URL=postgresql://user:pass@postgres:5432/skyhub_payment_db?connection_limit=10
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672
REDIS_URL=redis://redis:6379/2
STRIPE_SECRET_KEY=sk_test_<your-stripe-test-key>
STRIPE_WEBHOOK_SECRET=whsec_<your-webhook-secret>
CURRENCY=INR

# ── Notification Service ─────────────────────────────────────────────
REDIS_URL=redis://redis:6379/3
SENDGRID_API_KEY=SG.<your-sendgrid-key>
EMAIL_FROM=noreply@skyhub.com
BOOKING_SERVICE_INTERNAL_URL=http://booking-service:3003

# ── Shared ───────────────────────────────────────────────────────────
NODE_ENV=development
LOG_LEVEL=info
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318/v1/traces
```

### Env Validation on Startup

Every service validates its env vars using Zod before starting. If a required variable is missing, the process crashes immediately with a clear error — not silently at runtime:

```typescript
// packages/common-utils/src/validateEnv.ts
import { z } from 'zod';

export function validateEnv<T>(schema: z.ZodSchema<T>): T {
  const result = schema.safeParse(process.env);
  if (!result.success) {
    console.error('Invalid environment variables:\n', result.error.format());
    process.exit(1);
  }
  return result.data;
}

// Usage in each service's config/env.ts
export const env = validateEnv(z.object({
  PORT: z.string().transform(Number),
  DATABASE_URL: z.string().url(),
  REDIS_URL: z.string(),
  KAFKA_BROKERS: z.string(),
}));
```

---

## 15. Infrastructure & Local Development

### docker-compose.yml (Infrastructure Only — Services Run via Turbo)

```yaml
version: '3.9'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: skyhub
      POSTGRES_PASSWORD: skyhub_local
    ports: ["5432:5432"]
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init-databases.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U skyhub"]
      interval: 5s
      timeout: 5s
      retries: 5

  mongodb:
    image: mongo:7-jammy
    ports: ["27017:27017"]
    volumes:
      - mongo_data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 5s

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s

  kafka:
    image: confluentinc/cp-kafka:7.7.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server kafka:9092"]
      interval: 10s
      retries: 10

  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"    # Management UI at http://localhost:15672
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s

  jaeger:
    image: jaegertracing/all-in-one:1.57
    ports:
      - "16686:16686"    # Jaeger UI at http://localhost:16686
      - "4318:4318"      # OTLP HTTP endpoint

volumes:
  postgres_data:
  mongo_data:
```

**`scripts/init-databases.sql`** — Creates all 4 PostgreSQL databases:
```sql
CREATE DATABASE skyhub_user_db;
CREATE DATABASE skyhub_flight_db;
CREATE DATABASE skyhub_booking_db;
CREATE DATABASE skyhub_payment_db;
```

### Local Dev Workflow

```bash
# 1. Start all infrastructure (one time)
docker compose up -d

# 2. Install all workspace dependencies
npm install

# 3. Run database migrations for all services
npm run migrate:all

# 4. Seed admin users
npm run seed:users

# 5. Start all services in watch mode (Turbo parallel dev)
npm run dev

# Services available at:
# API Gateway:      http://localhost:3000
# User Service:     http://localhost:3001
# Flight Service:   http://localhost:3002
# Booking Service:  http://localhost:3003
# Payment Service:  http://localhost:3004
# Search Service:   http://localhost:3006
# RabbitMQ UI:      http://localhost:15672
# Jaeger UI:        http://localhost:16686
```

---

## 16. Build Phases Roadmap

| Phase | Services Built | Key Concepts Learned |
|---|---|---|
| **Phase 1** | `user-service` + `api-gateway` | RS256 JWT, refresh tokens, Redis blacklist, rate limiting, circuit breakers, Outbox pattern |
| **Phase 2** | `flight-service` | RBAC enforcement, Kafka producer, Outbox pattern, internal vs external routes |
| **Phase 3** | `search-service` | CQRS read model, Kafka consumer, tag-based cache invalidation, MongoDB indexes |
| **Phase 4** | `booking-service` | Saga orchestration, seat hold/expiry, transactional outbox, BullMQ, consumer idempotency |
| **Phase 5** | `payment-service` | Stripe PaymentIntents + webhooks, idempotency engine, minor-unit currency, refund flow |
| **Phase 6** | `notification-service` | BullMQ workers, PDFKit, SendGrid, DLQ, PII-safe job data |
| **Phase 7** | Shared packages | `shared-types`, `common-utils`, `message-broker` — extract and centralize shared code |
| **Phase 8** | Observability | Pino structured logging, prom-client metrics, OpenTelemetry traces, Grafana dashboards |

### Phase 1 Detailed Build Order (User Service + API Gateway)

1. **Bootstrap monorepo:** Fill in `services/user-service/package.json` and `tsconfig.json`. Set up Prisma with `schema.prisma`.
2. **Database models:** User, RefreshToken, OutboxEvent tables with all production columns (`email_verified`, `is_active`, `failed_login_attempts`, `locked_until`, `last_login_at`).
3. **Common utilities:** `AppError` class, Pino logger with `AsyncLocalStorage`, Zod env validator.
4. **Repository layer:** `user.repository.ts`, `token.repository.ts` — raw Prisma queries only.
5. **Service layer:** `auth.service.ts` (register, login, logout), `token.service.ts` (JWT sign/verify with RS256, refresh token management with hashed storage and rotation).
6. **Controller + Routes:** `auth.controller.ts`, Zod schemas, standard response envelope.
7. **Outbox Worker:** Polls `outbox_events`, publishes to Kafka, marks published.
8. **API Gateway:** Rate limiting, RS256 JWT verify via JWKS, Redis blacklist check, circuit breaker, proxy routes.
9. **Health checks:** `GET /health` on both services.
10. **Validation:** Full flow test — register → login → search with JWT → refresh → logout → verify blacklist.

---

> **This document is the living source of truth for SkyHub's architecture.** As implementation decisions evolve or new patterns are adopted, update this document first. Code is the implementation of this spec — not the other way around.

# 🗺️ SkyHub — Build & Learning Roadmap (Read This First)

> **Purpose of this document:** The design docs (`01`–`05`) describe the *finished* system. This document tells you **what order to build it in, how much of it to build at each step, and how to know you are done** with each step. Read a design doc like a map; read this doc like a GPS.

---

## 0. The Rules of This Project

These rules exist because the #1 way ambitious learning projects die is *building horizontally* (a little of every service) instead of *vertically* (one working slice at a time).

| Rule | What it means |
|---|---|
| **1. Vertical slices only** | Never start service N+1 until service N's "Definition of Done" checklist is fully green. |
| **2. v1 before v2** | Every service doc has a **Build Scope** section staging features into v1/v2/v3. Build only v1 the first time through. v2/v3 features are *retrofits* — retrofitting onto a live system is itself a top-5% skill. |
| **3. A feature is done when its test passes** | Not when it "works in Postman once." Each phase lists the 2–3 tests that *prove* the hard part works. Write at least those. |
| **4. Measure against the SLOs** | `01_Architecture.md` §1 defines p99 latency targets. At the end of each phase, run the load check listed in its Definition of Done. Top-5% developers don't say "it's fast" — they say "p99 is 140ms at 200 RPS." |
| **5. Update docs when reality diverges** | The docs are the source of truth *only if they are true*. If you make a different decision while coding, change the doc in the same commit. |
| **6. Local-first** | Everything runs via `docker-compose` (infra) + `turbo dev` (services). CI/CD, Kubernetes, and cloud deploys are deliberately **out of scope until Phase 8** — but write code as if CI existed: `npm run lint && npm run typecheck && npm test` must always pass at the repo root before you commit. That habit *is* CI, minus the server. |

### Foundations that are NOT optional (build in Phase 1, reuse everywhere)

These four things cost almost nothing when you have one service and are miserable to retrofit across seven:

1. **Zod-validated env config** — service crashes at startup with a clear message if an env var is missing/malformed. Never `Number(process.env.PORT) || 3002` silently defaulting.
2. **Pino structured logging** — no `console.log` anywhere. One logger instance from `@skyhub/common-utils`, correlation-ID aware (AsyncLocalStorage).
3. **Error pipeline** — `AppError` factories + `globalErrorHandler` + `notFoundHandler` + response envelope helpers from `@skyhub/common-utils`. Already written — *wire them in*.
4. **Layering** — `router → controller → service → repository`. Controllers never touch Prisma; repositories never throw HTTP errors.

---

## 1. Phase Order (and why it differs from the old roadmap)

The original roadmap said "User Service first." We are building **Flight Service first** — and that is the better learning order:

- Flight Service is pure CRUD + one genuinely hard problem (concurrent seat holds). You learn layering, Prisma, validation, and transactions *without* the cryptography and token state of auth.
- Auth (User Service + Gateway) comes second, so that by the time Booking needs `X-User-Id`, it exists.
- Kafka is deferred until something *consumes* it (Search Service). An outbox publishing to a topic nobody reads teaches you nothing.
- Payment is built first with a **fake payment consumer** (auto-approves after 2s) so you learn the saga mechanics in isolation, then swap in real Stripe. Swapping a fake for a real integration behind a stable message contract is exactly how real teams de-risk integrations.

```text
Phase 1  Flight Service v1          ← IN PROGRESS
Phase 2  User Service v1 + API Gateway v1
Phase 3  Search Service v1 (+ Kafka, + Outbox worker in Flight/User)
Phase 4  Booking Service v1 (+ RabbitMQ, + BullMQ, + fake payment consumer)
Phase 5  Payment Service v1 (real Stripe; delete the fake consumer)
Phase 6  Notification Service v1 (BullMQ workers, PDFKit, email)
Phase 7  v2/v3 retrofits (MFA, dynamic RBAC, fare quotes, refunds…) — pick what interests you
Phase 8  Hardening: metrics + tracing + load testing + CI/CD + containerized services
```

---

## 2. Phase 1 — Flight Service v1 (current)

**Read:** `04_Flight_Service_Design.md` (Build Scope section first, then §3 schema, then Features 8/9/10).

**Build:**
1. Foundations (env validation, pino, error pipeline, layering) — see §0 above.
2. Full Prisma schema: `Airport`, `Aircraft`, `FlightSchedule`, `FlightInstance`, `SeatInventory`, `SeatHold`, `OutboxEvent` — with a real migration (`prisma migrate dev`), not `db push`.
3. Seed script: ~10 airports, ~5 aircraft, ~20 schedules, ~60 instances with inventory. You cannot test search or booking later without data.
4. CRUD endpoints for airports / aircraft / schedules / instances (admin RBAC = just a header check for now; real JWT arrives in Phase 2).
5. **The crown jewel:** `hold-seats` / `release-seats` / `confirm-seats` internal endpoints with `SELECT … FOR UPDATE` + the `seat_holds` table.
6. **Hold-expiry sweeper:** a `setInterval` loop releasing expired ACTIVE holds (see 04 §Feature 9b). This is Flight Service's safety net; Booking's BullMQ timeout job (Phase 4) is the fast path.

**Defer (v2):** Kafka producer + outbox *worker* (write outbox rows now, publish them in Phase 3), `/metrics`, soft-delete flows.

**What you learn:** layered architecture, Prisma transactions + row locking, idempotency via state tables (not via event-log lookups), validation, migrations, seeding, background loops in Node.

**Definition of Done:**
- [ ] `npm run lint && npm run typecheck` clean at repo root
- [ ] **The concurrency test:** integration test fires 2 parallel `hold-seats` requests at an inventory with 1 seat left → exactly one 200, one 409. (Run it *without* `FOR UPDATE` once and watch it fail — that failure is the lesson.)
- [ ] **Idempotency test:** same `bookingId` held twice → second call returns the stored result, seats decremented only once.
- [ ] **Sweeper test:** hold with `heldUntil` in the past → sweeper releases it within one tick; a CONFIRMED hold is never touched.
- [ ] Health check returns DB status; graceful shutdown completes (with force-exit timeout).
- [ ] Load check: `autocannon -c 50 -d 10 http://localhost:3002/api/v1/airports` — p99 under 100ms.

---

## 3. Phase 2 — User Service v1 + API Gateway v1

**Read:** `02_User_Service_Design.md` Build Scope section. **Build only v1**: register, login, refresh + rotation, logout, `/me`, JWKS endpoint, static `role` enum on the user row.

**Explicitly defer:** email OTP verification, account lockout, MFA, dynamic RBAC tables (`roles`/`permissions`/join tables), sessions UI, audit logs. The schema can *include* the columns (cheap), but the endpoints wait for Phase 7.

**Gateway v1:** proxy routes, RS256 verify via JWKS, header injection (`X-User-Id`, `X-User-Role`, `X-User-Loyalty-Tier`, `X-User-Jti`, `X-User-Exp`, `X-Correlation-ID` — the jti/exp pair is what lets User Service blacklist tokens on logout), Redis rate limiting. Defer: circuit breakers, JWT blacklist (add with logout in a fast-follow), second gateway instance (a diagram concept — locally one is fine).

**What you learn:** bcrypt, RS256/JWKS asymmetric signing, refresh-token hashing + rotation, the trusted-header pattern, reverse proxying, distributed rate limiting.

**Definition of Done:**
- [ ] Full flow test: register → login → call Flight admin endpoint through the Gateway with JWT → refresh (old refresh token now dead) → logout (jti blacklisted, next call 401).
- [ ] **Token rotation test:** using a rotated-out refresh token returns 401.
- [ ] **Timing test:** login with a non-existent email takes ~the same time as a wrong password (dummy bcrypt compare).
- [ ] Flight Service admin routes now enforce real `X-User-Role` from the Gateway.

---

## 4. Phase 3 — Search Service v1 + Kafka goes live

**Read:** `03_Search_Service_Design.md` Build Scope section.

**Build:** Kafka + outbox **worker** in Flight Service (the rows are already being written since Phase 1 — now publish them); Search Service Kafka consumer → MongoDB read model; cache-aside Redis with tag-based invalidation; loyalty discount application via the **shared pricing function** (see §6 below — this matters for Phase 4).

**What you learn:** CQRS, eventual consistency you can *watch* (update a flight, see the search index catch up), consumer groups, at-least-once delivery + idempotent upserts, cache stampedes and invalidation.

**Definition of Done:**
- [ ] Update a flight price via admin API → within seconds the search result reflects it and the stale cache entry is gone (tag-based, no `KEYS`).
- [ ] Kill the Search Service, update 3 flights, restart it → consumer catches up from its committed offset; read model converges.
- [ ] **Duplicate-event test:** replay the same FLIGHT_UPDATED event twice → MongoDB state identical (idempotent upsert).
- [ ] Load check vs SLO: cached search p99 < 150ms, cache-miss p99 < 800ms (`autocannon`/`k6`).

---

## 5. Phase 4 — Booking Service v1 (saga, with fake payment)

**Read:** `05_Booking_Service_Design.md` Build Scope section.

**Build:** create booking (sync seat hold → booking row + saga log + outbox, all in one transaction); RabbitMQ publish `BOOKING_INITIATED`; **fake payment consumer** (a 30-line worker that consumes `booking.initiated`, waits 2s, publishes `PAYMENT_SUCCESS` 80% / `PAYMENT_FAILED` 20%); payment-result consumer (confirm or rollback); on confirm → call Flight `confirm-seats`; BullMQ seat-timeout job; price re-validation (§6).

**What you learn:** saga orchestration, compensating transactions, the transactional outbox end-to-end, consumer idempotency, delayed jobs, designing message contracts *before* the real implementation exists.

**Definition of Done:**
- [ ] Happy path: book → fake payment succeeds → booking CONFIRMED → Flight hold is CONFIRMED → timeout job cancelled.
- [ ] Rollback path: fake payment fails → booking CANCELLED → seats released exactly once.
- [ ] Timeout path: set hold duration to 1 min, never "pay" → booking TIMED_OUT, seats back.
- [ ] **The chaos test:** kill Booking Service after the booking transaction commits but before any message is consumed → restart → outbox worker publishes, saga completes. Nothing lost.
- [ ] **Price test:** booking total equals exactly what Search displayed for the same user (same shared pricing function on both sides).

---

## 6. The Price Consistency Rule (applies from Phase 3 onward)

**The bug this prevents:** Search shows a GOLD user ₹9,000 (10% off ₹10,000). Booking fetches the base price from Flight Service (₹10,000) and charges that. User screams.

**v1 rule:** the discount is a *pure function* of `(basePrice, loyaltyTier)` living in **`@skyhub/common-utils`** (`calculateFinalPrice()`). Search uses it for display; Booking uses it to compute the charge from the authoritative base price + the `X-User-Loyalty-Tier` header. Same inputs → same output → consistent price, no extra network call.

**v2 (Phase 7 option):** signed, short-lived **fare quotes** — Search returns a quote token (price + tier + expiry, HMAC-signed); Booking validates the token instead of recomputing. This is how real airlines/OTAs handle it (fare quote / fare lock) and protects against the base price changing between search and booking.

---

## 7. Phases 5–8 (summary)

- **Phase 5 — Payment Service:** Stripe PaymentIntents + webhook signature verification + Redis idempotency keys. Delete the fake consumer; the message contract doesn't change — that's the payoff. *Done when:* Stripe test-mode card succeeds end-to-end; replayed webhook is a no-op; same Idempotency-Key twice → one PaymentIntent.
- **Phase 6 — Notification Service:** BullMQ workers, PDFKit ticket, email (use [Mailpit](https://github.com/axllent/mailpit) locally — real SendGrid is a config swap), retries → DLQ. *Done when:* a confirmed booking lands a PDF email in Mailpit; a forced email failure retries 3× then parks in the DLQ where you can inspect and replay it.
- **Phase 7 — Retrofits:** pick from: User v2/v3 (lockout, OTP, MFA, dynamic RBAC), fare quotes, refunds, booking reminders, circuit breakers + axios-retry on Booking→Flight, JWT blacklist if deferred. Each one practices changing a *running* system.
- **Phase 8 — Hardening:** prom-client `/metrics` + Grafana, OpenTelemetry → Jaeger, k6 load suite vs every SLO, Dockerfiles for services, GitHub Actions CI. Also: generate OpenAPI from your Zod schemas (`zod-openapi`) and serve Swagger UI per service.

---

## 8. Redis Topology (corrected — read before Phase 2)

The old design put cache + JWT blacklist + idempotency + BullMQ on one Redis instance with different logical DBs and *different eviction policies per DB*. **That is impossible** — `maxmemory-policy` is per-instance, not per-DB. Under memory pressure, an `allkeys-lru` instance could evict JWT blacklist entries (logged-out tokens come back to life) or BullMQ job state (BullMQ's docs *require* `noeviction`).

**Corrected local topology — two Redis containers:**

| Instance | Policy | Port | Holds |
|---|---|---|---|
| `redis-core` | `noeviction` | 6379 | DB 0: rate limits + JWT blacklist · DB 2: payment idempotency · DB 3: BullMQ |
| `redis-cache` | `allkeys-lru` | 6380 | DB 0: search cache + invalidation tags |

Also note for interviews: **Redis Cluster supports only DB 0** — at real scale, logical DBs disappear and you separate by instance anyway. Our two-instance layout is the production-realistic shape.

---

## 9. Daily Workflow

```powershell
docker compose up -d          # infra (postgres, mongo, redis x2, kafka, rabbitmq, mailpit)
npm install
npm run dev                   # turbo runs all current services in watch mode
# before every commit:
npm run lint; npm run typecheck; npm test
```

Keep a `requests/` folder of `.http` files (VS Code REST Client) per service — executable, committed documentation of every endpoint. Add each new endpoint's request the moment it works.

**Commit style:** small commits, one logical change each, `feat(flight-service): …` format you're already using. When a phase's Definition of Done is green, tag it: `git tag phase-1-complete`.

# SkyHub — Flight Service: Complete Production-Grade Build Guide

## Table of Contents

1. [Bounded Context & Responsibility](#1-bounded-context--responsibility)
2. [Complete Feature List](#2-complete-feature-list)
3. [Database Design & Prisma Schema](#3-database-design--prisma-schema)
4. [Security & RBAC Architecture](#4-security--rbac-architecture)
5. [Complete REST API Specification](#5-complete-rest-api-specification)
6. [Zod Validation Schemas](#6-zod-validation-schemas)
7. [Kafka Event Publishing (Outbox Pattern)](#7-kafka-event-publishing-outbox-pattern)
8. [Layered Architecture & File Map](#8-layered-architecture--file-map)
9. [npm Dependencies](#9-npm-dependencies)
10. [Environment Variables](#10-environment-variables)
11. [Step-by-Step Build Plan](#11-step-by-step-build-plan)
12. [Testing Strategy](#12-testing-strategy)

---

## 1. Bounded Context & Responsibility

The Flight Service is the **exclusive write-side owner of the flight catalog and seat inventory**. It is the single source of truth for everything flight-related. No other service may write to `skyhub_flight_db` or modify `available_seats` directly.

```
ADMIN WRITE PATH (admin creates/updates a flight)

ADMIN CLIENT
  └── POST /api/v1/flights ────────────────── API GATEWAY
                                1. Verify JWT
                                2. Check X-User-Role = FLIGHT_ADMIN | SUPER_ADMIN
                                3. Proxy to FLIGHT SERVICE
                                               │
                                    FLIGHT SERVICE (Port 3002)
                                4. Zod validate body
                                5. BEGIN TRANSACTION
                                     INSERT INTO flights (...)
                                     INSERT INTO outbox_events (FLIGHT_UPDATED, ...)
                                   COMMIT
                                6. Return 201 Created
                       (Background: Outbox Worker)
                                7. Publish FLIGHT_UPDATED → Kafka: flight-inventory-events
                                8. Mark outbox event PUBLISHED

  ✅ Admin sees: 201 immediately
  ✅ Search Service: receives Kafka event → upserts MongoDB → invalidates Redis cache

─────────────────────────────────────────────────────────────────────

INTERNAL SEAT HOLD PATH (Booking Service holds seats before payment)

BOOKING SERVICE (internal network only — never via Gateway)
  └── PATCH /internal/flights/:id/hold-seats { seats: 2, bookingId: "..." }
                                               │
                                    FLIGHT SERVICE
                                1. BEGIN TRANSACTION
                                     SELECT ... FROM flights WHERE id = ? FOR UPDATE
                                     ← exclusive row lock, blocks concurrent holds
                                     IF available_seats < 2 → ROLLBACK → 409 Conflict
                                     UPDATE SET available_seats = available_seats - 2
                                     INSERT INTO outbox_events (SEATS_HELD, ...)
                                   COMMIT
                                2. Return 200 { remainingSeats, heldUntil }

  ✅ Booking Service: proceeds to create booking record
  ✅ Search Service: receives SEATS_HELD event → updates MongoDB → invalidates cache
```

**Hard boundaries — what this service owns and what it does not touch:**

| Owns | Does NOT own |
|---|---|
| `skyhub_flight_db` (PostgreSQL, exclusive) | Any other service's database |
| Flight catalog (create, update, cancel) | Booking records |
| Seat inventory write operations | Payment records |
| `flight-inventory-events` Kafka topic (producer only) | Search index (Search Service owns it) |
| Outbox events for seat changes | JWT signing or user identity |
| Internal hold/release endpoints | Loyalty tier management |

**Data contract with other services:**
- **Search Service** receives flight data exclusively via Kafka events (`FLIGHT_UPDATED`, `SEATS_HELD`, `SEATS_RELEASED`, `FLIGHT_CANCELLED`)
- **Booking Service** calls Flight Service synchronously via HTTP internal endpoints (`/internal/flights/:id/hold-seats`, `/internal/flights/:id/release-seats`) — these are the ONLY cross-service HTTP calls Flight Service receives
- **API Gateway** proxies public read + admin write routes to Flight Service

---

## 2. Complete Feature List

### Feature 1: Create Flight (Admin)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN` role (enforced by `requireRole` middleware reading `X-User-Role` header injected by Gateway)

**Flow:**
1. Zod validates the full flight body (see Section 6 for all rules)
2. Check for duplicate `flight_number` on the same `departure_date` → 409 Conflict if found (same flight number cannot depart twice on the same date)
3. Validate `available_seats <= total_seats`
4. Validate `origin !== destination`
5. Validate `departure_date` is today or in the future
6. Validate that `arrival_time` / `arrival_date` is after `departure_time` / `departure_date`
7. Validate `base_price > 0` (integer paise — never a float)
8. In ONE atomic DB transaction:
   - `INSERT INTO flights (...)` with status `ACTIVE`
   - `INSERT INTO outbox_events (event_type='FLIGHT_UPDATED', payload={full flight object})`
9. Return `201 Created` with the new flight object

**Why write to outbox in the same transaction?**
If the service crashes after inserting the flight but before publishing to Kafka, the Outbox Worker will pick up the pending event on restart. Without the outbox, the Search Service would never know this flight exists.

---

### Feature 2: Update Flight (Admin)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**What is updatable:**
- `basePrice` — price change (e.g., promotional discount)
- `departureTime`, `arrivalTime`, `arrivalDate` — schedule change (delayed flight)
- `aircraft` — equipment swap
- `amenities`, `baggageAllowance`, `refundable` — service change
- `totalSeats`, `availableSeats` — capacity change (e.g., aircraft upgraded to larger plane)
- `status` — ACTIVE → DELAYED (marks the flight as delayed, still bookable)

**What is NOT updatable after creation:**
- `flightNumber` — changing the flight number creates a new flight
- `origin`, `destination`, `departureDate`, `cabinClass` — these define the flight identity; a change to any of these means cancel and recreate

**Flow:**
1. Zod validates partial body (all fields optional)
2. Fetch flight by ID → 404 if not found
3. If updating `availableSeats`: validate `newAvailableSeats <= totalSeats` (or `newTotalSeats` if also updating)
4. If updating `status` to `CANCELLED`: reject — use the dedicated cancel endpoint (Feature 4)
5. In ONE transaction:
   - `UPDATE flights SET ...`
   - `INSERT INTO outbox_events (FLIGHT_UPDATED, {full updated flight object})`
6. Return `200 OK` with the updated flight

---

### Feature 3: Get Flight by ID (Public)

**Who can call:** Anyone — no auth required (public read endpoint)

**Flow:**
1. Fetch flight by `id` (UUID)
2. If status is `CANCELLED`: return 404 (treat cancelled flights as non-existent for public consumers)
3. Return flight detail

**Why is this endpoint public?**
A user browsing the booking confirmation page needs to see the flight they are about to pay for. Requiring a JWT for a flight detail read is unnecessary friction and adds rate-limit pressure on auth routes.

---

### Feature 4: List Flights (Admin Only)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Purpose:** Admin panel flight management — shows ALL flights including `CANCELLED` and `DELAYED` status that the public search hides.

**Supported query filters:**
- `origin`, `destination` — IATA code filter
- `date` — departure date filter
- `cabin` — cabin class filter
- `airline` — airline name filter
- `status` — `ACTIVE` | `DELAYED` | `CANCELLED` (default: all)
- `page`, `limit` — pagination (default: page=1, limit=20, max=100)
- `sortBy` — `departureDate` | `basePrice` | `createdAt` (default: `createdAt DESC`)

**Response:** paginated list of flights with full detail including `availableSeats`, `createdBy`, `createdAt`

---

### Feature 5: Cancel Flight (Admin)

**Who can call:** `SUPER_ADMIN` only (cancellation is irreversible — higher privilege required)

**Flow:**
1. Fetch flight by ID → 404 if not found
2. Check `status !== 'CANCELLED'` → 400 "Already cancelled" if already done
3. In ONE transaction:
   - `UPDATE flights SET status = 'CANCELLED', available_seats = 0`
   - `INSERT INTO outbox_events (FLIGHT_CANCELLED, { flightId, origin, destination, departureDate })`
4. Return `200 OK`

**Search Service reaction to `FLIGHT_CANCELLED`:**
The Kafka consumer in Search Service receives this event and removes the flight document from MongoDB. Any active cache entries for that route/date are also invalidated via tag lookup.

**Booking Service consideration:**
Cancelling a flight with existing confirmed bookings should trigger compensating transactions (refunds). For Phase 2 (this service), the Flight Service only publishes the event. The Booking Service (Phase 4) consumes `FLIGHT_CANCELLED` events and initiates refund sagas. This is noted in the outbox payload — include `hasActiveBookings` flag if needed by downstream consumers.

---

### Feature 6: Hold Seats (Internal — Booking Service Only)

**Who can call:** Only Booking Service, via internal HTTP (not proxied by Gateway)

**Why this is the most critical endpoint in the service:**
This is where the ACID guarantee lives. Two users requesting to book the last 2 seats at the same millisecond must result in exactly one success and one 409 Conflict. This requires a database-level exclusive row lock — not optimistic locking, not application-level mutex, not Redis lock.

**Flow using `SELECT ... FOR UPDATE`:**
```
1. Booking Service sends:
   PATCH /internal/flights/{flightId}/hold-seats
   Body: { seats: 2, bookingId: "booking-uuid" }

2. Flight Service executes (inside Prisma $transaction):

   Step A: Acquire row lock
   SELECT id, available_seats
   FROM flights
   WHERE id = {flightId}
   FOR UPDATE  ← PostgreSQL acquires an exclusive lock on this row
                  All other concurrent SELECT...FOR UPDATE on this row WAIT here
                  until this transaction commits or rolls back

   Step B: Check availability
   IF flight.available_seats < 2:
     ROLLBACK (lock released)
     Return 409 { code: 'INSUFFICIENT_SEATS', availableSeats: flight.available_seats }

   Step C: Decrement seats + write outbox (atomic)
   UPDATE flights
     SET available_seats = available_seats - 2
   WHERE id = {flightId}

   INSERT INTO outbox_events
     (event_type='SEATS_HELD',
      payload={ flightId, seatsHeld: 2, remainingSeats: updated.available_seats,
                heldUntil: NOW() + 15 minutes, bookingId })

   COMMIT (lock released — next waiting transaction can proceed)

3. Return 200 {
     success: true,
     remainingSeats: <updated count>,
     heldUntil: <NOW + 15 minutes ISO string>
   }
```

**What `heldUntil` means:**
The Flight Service does NOT manage hold expiry — that is the Booking Service's job (via BullMQ `seat-timeout-queue`). The Flight Service simply reports when the hold should expire so the Booking Service can schedule the expiry job. The Flight Service never auto-releases seats — that is always triggered externally.

**Concurrent hold behaviour:**
- Request A arrives: acquires lock, sees 10 available seats, decrements to 8, commits, releases lock
- Request B (arrived 2ms later): was waiting at `FOR UPDATE`, now acquires lock, sees 8 seats (not 10), proceeds
- This is sequential ACID correctness — not a race condition

---

### Feature 7: Release Seats (Internal — Booking Service Only)

**Triggered by two scenarios:**
1. Booking Service saga rollback (payment failed)
2. Seat hold timeout (user abandoned payment — BullMQ `seat-timeout-queue` fires)

**Flow:**
```
1. Booking Service sends:
   PATCH /internal/flights/{flightId}/release-seats
   Body: { seats: 2, bookingId: "booking-uuid" }

2. Flight Service executes (inside Prisma $transaction):

   Fetch flight by ID → 404 if not found
   (No FOR UPDATE lock needed here — incrementing seats cannot cause data integrity violation)

   UPDATE flights
     SET available_seats = LEAST(available_seats + seats, total_seats)
   WHERE id = {flightId}

   ← LEAST() is a safety guard: available_seats can never exceed total_seats
     even if release is called twice (double-release idempotency guard)

   INSERT INTO outbox_events
     (event_type='SEATS_RELEASED',
      payload={ flightId, seatsReleased: seats, remainingSeats: updated.available_seats, bookingId })

   COMMIT

3. Return 200 { success: true, remainingSeats: <updated count> }
```

**Why no lock on release?**
Releasing seats is safe without a lock because incrementing an integer cannot double-book a seat. Two concurrent releases would both add their seats back — the result is correct regardless of order. The `LEAST(available + n, total)` guard prevents `available_seats` from exceeding `total_seats` even if the Booking Service accidentally calls release twice.

**Idempotency:**
The Booking Service uses `bookingId` in the payload. If the release HTTP call fails and is retried, the second call will run `LEAST(available + seats, total)` again — a no-op if seats are already back to `total`. This makes the endpoint safe to retry.

---

### Feature 8: Outbox Worker (Background)

Runs as a `setInterval` loop every 5 seconds inside the service process.

**What it does:**
1. `SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 100`
2. For each pending event:
   - Serialize as standard Kafka envelope (see Section 7)
   - Publish to Kafka topic `flight-inventory-events`
   - On success: `UPDATE outbox_events SET status='PUBLISHED', published_at=NOW()`
   - On failure: log error, leave as PENDING (will retry next interval)
3. Never deletes outbox rows — keep for audit trail and debugging

---

### Feature 9: Health Check

**`GET /health`** — checks PostgreSQL connection and Kafka producer connection

---

### Feature 10: Metrics

**`GET /metrics`** — Prometheus scrape format

Flight Service-specific metrics:
- `flight_hold_requests_total{result}` — counter: `result=success | insufficient_seats | not_found`
- `flight_release_requests_total` — counter
- `outbox_pending_count` — gauge: how many outbox events are awaiting publish (alert if this grows > 100)
- `outbox_publish_duration_ms` — histogram: time to publish each batch

---

## 3. Database Design & Prisma Schema

### 3.1 Entity-Relationship Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                           FLIGHTS                              │
├────────────────────────────────────────────────────────────────┤
│ id                    UUID         PK                          │
│ flight_number         VARCHAR(20)  NOT NULL                    │
│ airline               VARCHAR(100) NOT NULL                    │
│ origin                VARCHAR(3)   NOT NULL  (IATA code)       │
│ destination           VARCHAR(3)   NOT NULL  (IATA code)       │
│ departure_date        DATE         NOT NULL                    │
│ departure_time        TIME         NOT NULL                    │
│ arrival_date          DATE         NOT NULL                    │
│ arrival_time          TIME         NOT NULL                    │
│ duration_minutes      INT          NOT NULL                    │
│ cabin_class           ENUM         NOT NULL                    │
│ base_price            INT          NOT NULL  (paise)           │
│ total_seats           INT          NOT NULL                    │
│ available_seats       INT          NOT NULL  (≤ total_seats)   │
│ aircraft              VARCHAR(100) NULL                        │
│ stops                 INT          DEFAULT 0                   │
│ amenities             JSONB        DEFAULT []                  │
│ baggage_allowance     JSONB        NOT NULL                    │
│ refundable            BOOLEAN      DEFAULT false               │
│ status                ENUM         DEFAULT ACTIVE              │
│ created_by_id         UUID         NOT NULL  FK → users.id     │
│ created_at            TIMESTAMPTZ  DEFAULT NOW()               │
│ updated_at            TIMESTAMPTZ  AUTO UPDATE                 │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│                       OUTBOX_EVENTS                            │
├────────────────────────────────────────────────────────────────┤
│ id           UUID         PK                                   │
│ event_type   VARCHAR(100) NOT NULL                             │
│ payload      JSONB        NOT NULL                             │
│ status       ENUM         DEFAULT PENDING                      │
│ created_at   TIMESTAMPTZ  DEFAULT NOW()                        │
│ published_at TIMESTAMPTZ  NULL                                 │
└────────────────────────────────────────────────────────────────┘
```

**Note on `created_by_id`:**
The Flight Service does NOT call User Service to validate this UUID. It trusts the `X-User-Id` header injected by the Gateway (which has already verified the JWT). The UUID is stored for audit purposes — which admin created which flight.

**Why no `FLIGHT_SCHEDULES` table?**
Some flight booking systems separate "schedule" (recurring route definition) from "instance" (specific flight on a specific date). SkyHub keeps it simple: each row in `flights` is a single flight instance on a specific date. This is sufficient for Phase 2. A recurring schedule feature can be added in a future phase without schema changes to the booking or payment layers.

### 3.2 Column-by-Column Justification

#### `flights` table

| Column | Type | Why This Design |
|---|---|---|
| `id` | UUID | Globally unique, no sequence conflicts. Exposed in URLs and Kafka events. |
| `flight_number` | VARCHAR(20) | Industry format: carrier code + number (e.g., `6E-204`, `AI-102`). Not globally unique — same flight number can exist on different dates. Unique constraint is on `(flight_number, departure_date, cabin_class)`. |
| `origin` / `destination` | VARCHAR(3) | IATA 3-letter airport codes. Always stored uppercase. |
| `departure_date` | DATE | Stored as DATE (not DATETIME) so date-only searches use an exact B-Tree equality match, not a range scan. The `departure_time` column holds the time separately. |
| `departure_time` | TIME | PostgreSQL `TIME` type: `HH:MM:SS`. No timezone — assumed to be local airport time. This matches how airlines publish schedules. |
| `arrival_date` | DATE | Stored separately from departure date to handle overnight flights where arrival is the next calendar day. |
| `duration_minutes` | INT | Pre-computed at insert time. Avoids recomputing on every read. Computed as: `(arrival_date + arrival_time) - (departure_date + departure_time)` in minutes. Validated at insert: `duration_minutes > 0`. |
| `cabin_class` | ENUM | `ECONOMY \| BUSINESS \| FIRST`. ENUM enforces at DB level. A flight record represents a single cabin — a single aircraft offering ECONOMY and BUSINESS is stored as TWO separate flight rows (same flight_number, different cabin_class). This simplifies seat counting. |
| `base_price` | INT | Minor units (paise for INR). **Never a float.** `₹4,999 = 499900`. All discount/pricing calculations use integer arithmetic. |
| `total_seats` | INT | Immutable after creation (unless aircraft is swapped via update). The physical seat capacity. |
| `available_seats` | INT | Changes on every hold/release. Must always be `0 ≤ available_seats ≤ total_seats`. DB-level check constraint enforces this. |
| `amenities` | JSONB | Flexible array of strings: `["wifi", "meal", "usb", "entertainment"]`. JSONB is indexed in PostgreSQL if needed. Stored here so Search Service can show them without a join. |
| `baggage_allowance` | JSONB | `{ "cabin": "7kg", "checked": "15kg" }`. JSONB allows adding more fields (e.g., `"excess_per_kg_price"`) later without a migration. |
| `status` | ENUM | `ACTIVE \| DELAYED \| CANCELLED`. `CANCELLED` flights have `available_seats = 0` and are hidden from public endpoints. `DELAYED` flights are still bookable — only schedule times are different. |
| `created_by_id` | UUID | The admin user who created this flight. Stored for audit trail. Not a FK constraint at DB level (Flight DB doesn't have a users table), but logically references `user_id` from User Service. |
| `created_at` / `updated_at` | TIMESTAMPTZ | Timezone-aware timestamps. `TIMESTAMPTZ` stores in UTC internally, converts to local on read. Never use bare `TIMESTAMP`. |

#### `outbox_events` table

Same design as User Service outbox. Identical columns and index strategy.

| Column | Why |
|---|---|
| `event_type` | String (not ENUM) — add new event types without migration |
| `payload` | JSONB — full Kafka envelope stored |
| `status` | ENUM: `PENDING \| PUBLISHED \| FAILED` |
| Compound index on `(status, created_at)` | Outbox Worker query: `WHERE status='PENDING' ORDER BY created_at ASC LIMIT 100` |

### 3.3 Database Constraints (CHECK constraints)

These are enforced at DB level, not just application level:

```sql
-- available_seats must be non-negative and cannot exceed total_seats
ALTER TABLE flights
  ADD CONSTRAINT chk_available_seats
  CHECK (available_seats >= 0 AND available_seats <= total_seats);

-- base_price must be positive
ALTER TABLE flights
  ADD CONSTRAINT chk_base_price
  CHECK (base_price > 0);

-- total_seats must be positive
ALTER TABLE flights
  ADD CONSTRAINT chk_total_seats
  CHECK (total_seats > 0);

-- duration must be positive
ALTER TABLE flights
  ADD CONSTRAINT chk_duration
  CHECK (duration_minutes > 0);
```

In Prisma, CHECK constraints are added via `@@check` or raw SQL in migrations. Since Prisma does not natively support `@@check`, add them in the initial migration SQL file.

### 3.4 Complete Prisma Schema

**File: `services/flight-service/prisma/schema.prisma`**

```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

// ─── Enums ────────────────────────────────────────────────────────────────────

enum CabinClass {
  ECONOMY
  BUSINESS
  FIRST
}

enum FlightStatus {
  ACTIVE
  DELAYED
  CANCELLED
}

enum OutboxStatus {
  PENDING
  PUBLISHED
  FAILED
}

// ─── Models ───────────────────────────────────────────────────────────────────

model Flight {
  id              String       @id @default(uuid())
  flightNumber    String       @map("flight_number")
  airline         String
  origin          String       // IATA code, always uppercase
  destination     String       // IATA code, always uppercase
  departureDate   DateTime     @map("departure_date")  @db.Date
  departureTime   DateTime     @map("departure_time")  @db.Time(0)
  arrivalDate     DateTime     @map("arrival_date")    @db.Date
  arrivalTime     DateTime     @map("arrival_time")    @db.Time(0)
  durationMinutes Int          @map("duration_minutes")
  cabinClass      CabinClass   @map("cabin_class")
  basePrice       Int          @map("base_price")        // paise — never float
  totalSeats      Int          @map("total_seats")
  availableSeats  Int          @map("available_seats")
  aircraft        String?
  stops           Int          @default(0)
  amenities       Json         @default("[]")
  baggageAllowance Json        @map("baggage_allowance")
  refundable      Boolean      @default(false)
  status          FlightStatus @default(ACTIVE)
  createdById     String       @map("created_by_id")  // admin userId from JWT
  createdAt       DateTime     @default(now()) @map("created_at")
  updatedAt       DateTime     @updatedAt              @map("updated_at")

  @@unique([flightNumber, departureDate, cabinClass], name: "uq_flight_identity")
  @@index([origin, destination, departureDate, cabinClass], name: "idx_flight_search")
  @@index([status])
  @@index([departureDate])
  @@map("flights")
}

model OutboxEvent {
  id          String       @id @default(uuid())
  eventType   String       @map("event_type")
  payload     Json
  status      OutboxStatus @default(PENDING)
  createdAt   DateTime     @default(now()) @map("created_at")
  publishedAt DateTime?    @map("published_at")

  @@index([status, createdAt])
  @@map("outbox_events")
}
```

### 3.5 Index Summary

| Index | Columns | Type | Purpose |
|---|---|---|---|
| `uq_flight_identity` | `(flightNumber, departureDate, cabinClass)` | Unique | Prevent duplicate flight creation |
| `idx_flight_search` | `(origin, destination, departureDate, cabinClass)` | Compound B-Tree | Admin list filtering (mirrors Search Service MongoDB index) |
| `idx_status` | `status` | B-Tree | Admin list filter by status |
| `idx_departure_date` | `departureDate` | B-Tree | Admin list sort/filter by date |
| `(status, createdAt)` on outbox | Compound | B-Tree | Outbox Worker polling query |

**Why the unique constraint on `(flightNumber, departureDate, cabinClass)`?**
Flight `6E-204` on `2026-10-12` can exist in ECONOMY and BUSINESS — those are two separate rows (different seat counts, different prices). But creating two ECONOMY rows for `6E-204` on the same date would be a data error — that is what the unique constraint prevents.

---

## 4. Security & RBAC Architecture

### 4.1 Role Enforcement on Admin Routes

The Flight Service trusts `X-User-Role` and `X-User-Id` headers injected by the API Gateway. It does NOT re-verify the JWT — that is the Gateway's job.

**`requireRole` middleware:**
```
Read X-User-Role header
  └── If header missing → 401 Unauthorized ("Authentication required")
  └── If role not in allowed list → 403 Forbidden ("Insufficient permissions")
  └── If role matches → attach to req.userRole + req.userId, call next()
```

**Role requirements per endpoint:**

| Endpoint | Minimum Role |
|---|---|
| `POST /api/v1/flights` | `FLIGHT_ADMIN` |
| `PATCH /api/v1/flights/:id` | `FLIGHT_ADMIN` |
| `GET /api/v1/flights` (list, all statuses) | `FLIGHT_ADMIN` |
| `DELETE /api/v1/flights/:id` (cancel) | `SUPER_ADMIN` |
| `GET /api/v1/flights/:id` (single detail) | No auth required |
| `/internal/flights/:id/hold-seats` | No JWT check (internal network only) |
| `/internal/flights/:id/release-seats` | No JWT check (internal network only) |

### 4.2 Internal Endpoint Protection

The `/internal/*` routes are **never proxied by the API Gateway**. The Gateway only maps:
- `/api/v1/flights` → flight-service
- `/api/v1/flights/:id` → flight-service

Not `/internal/*`.

**In local development:** internal routes are reachable at `http://localhost:3002/internal/...`. Only the Booking Service calls them. No external client knows this port.

**In Kubernetes production:** NetworkPolicy restricts which pods can reach port 3002. Only pods with label `app: booking-service` can call `service: flight-service:3002/internal/...`. mTLS (Istio/Linkerd) adds certificate-based mutual authentication.

**In the Express app:**
Mount internal and public routes on separate prefixes:
```
app.use('/api/v1',    publicRouter)     ← Gateway proxies these
app.use('/internal',  internalRouter)   ← Only internal services call these
app.use('/',          healthRouter)     ← /health, /metrics
```

### 4.3 Input Validation Security

- All admin input validated with Zod before any DB operation
- IATA codes normalized to uppercase (prevent case-sensitivity bugs)
- `base_price` validated as positive integer (prevents negative pricing bugs)
- `flightNumber` sanitized — reject strings with SQL injection patterns (Prisma parameterizes queries, but belt-and-suspenders)
- `amenities` array maximum length: 20 items. Each item maximum 50 chars (prevent oversized JSONB)
- `baggage_allowance` keys whitelisted (`cabin`, `checked`, `excess_per_kg`) — reject unknown keys

---

## 5. Complete REST API Specification

All public endpoints are prefixed `/api/v1` at the Gateway level. The Flight Service handles them directly.

### Standard Response Envelope

Same as the cluster-wide standard:
```json
// Success
{ "success": true, "message": "...", "data": {}, "meta": {}, "traceId": "..." }

// Error
{ "success": false, "error": { "code": "...", "message": "...", "details": [] }, "traceId": "..." }
```

---

### Endpoint 1: POST /api/v1/flights

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body:**
```json
{
  "flightNumber":    "6E-204",
  "airline":         "IndiGo",
  "origin":          "DEL",
  "destination":     "BOM",
  "departureDate":   "2026-10-12",
  "departureTime":   "06:30",
  "arrivalDate":     "2026-10-12",
  "arrivalTime":     "09:15",
  "durationMinutes": 165,
  "cabinClass":      "ECONOMY",
  "basePrice":       499900,
  "totalSeats":      180,
  "availableSeats":  180,
  "aircraft":        "Airbus A320",
  "stops":           0,
  "amenities":       ["usb", "snack"],
  "baggageAllowance": {
    "cabin":   "7kg",
    "checked": "15kg"
  },
  "refundable": false
}
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Flight created successfully.",
  "data": {
    "flightId":        "abc123-def456-ghi789",
    "flightNumber":    "6E-204",
    "airline":         "IndiGo",
    "origin":          "DEL",
    "destination":     "BOM",
    "departureDate":   "2026-10-12",
    "departureTime":   "06:30",
    "arrivalDate":     "2026-10-12",
    "arrivalTime":     "09:15",
    "durationMinutes": 165,
    "cabinClass":      "ECONOMY",
    "basePrice":       499900,
    "totalSeats":      180,
    "availableSeats":  180,
    "aircraft":        "Airbus A320",
    "stops":           0,
    "amenities":       ["usb", "snack"],
    "baggageAllowance": { "cabin": "7kg", "checked": "15kg" },
    "refundable":      false,
    "status":          "ACTIVE",
    "createdById":     "7b58c281-a5bf-4050-a922-a72a1cd40a92",
    "createdAt":       "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR          → Zod validation failed (see Section 6)
409 CONFLICT                  → Same flight number + date + cabin already exists
422 BUSINESS_RULE_VIOLATION   → origin === destination
                              → departureDate is in the past
                              → availableSeats > totalSeats
                              → arrival is before departure
500 INTERNAL_ERROR
```

---

### Endpoint 2: PATCH /api/v1/flights/:id

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body (all fields optional — only send what needs updating):**
```json
{
  "basePrice":       449900,
  "departureTime":   "07:00",
  "arrivalTime":     "09:45",
  "durationMinutes": 165,
  "aircraft":        "Boeing 737",
  "amenities":       ["wifi", "meal", "usb"],
  "baggageAllowance": { "cabin": "7kg", "checked": "20kg" },
  "status":          "DELAYED"
}
```

**Note:** `origin`, `destination`, `departureDate`, `cabinClass`, `flightNumber` are not accepted — attempting to send them returns a 400 validation error with a clear message: `"Flight identity fields (origin, destination, departureDate, cabinClass, flightNumber) cannot be updated. Cancel and recreate the flight instead."`

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight updated successfully.",
  "data": { /* full updated flight object, same shape as create response */ },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR          → invalid field or disallowed field sent
404 NOT_FOUND                 → flight not found by ID
400 CANNOT_UPDATE_CANCELLED   → attempt to update a CANCELLED flight
422 BUSINESS_RULE_VIOLATION   → availableSeats > totalSeats after update
```

---

### Endpoint 3: GET /api/v1/flights/:id

**Auth required:** No

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight retrieved successfully.",
  "data": {
    "flightId":        "abc123-def456-ghi789",
    "flightNumber":    "6E-204",
    "airline":         "IndiGo",
    "origin":          "DEL",
    "destination":     "BOM",
    "departureDate":   "2026-10-12",
    "departureTime":   "06:30",
    "arrivalDate":     "2026-10-12",
    "arrivalTime":     "09:15",
    "durationMinutes": 165,
    "cabinClass":      "ECONOMY",
    "basePrice":       499900,
    "totalSeats":      180,
    "availableSeats":  142,
    "aircraft":        "Airbus A320",
    "stops":           0,
    "amenities":       ["usb", "snack"],
    "baggageAllowance": { "cabin": "7kg", "checked": "15kg" },
    "refundable":      false,
    "status":          "ACTIVE"
  },
  "traceId": "tr-f47ac10b"
}
```

**Note:** `createdById` is NOT returned in the public response — that is internal admin data.

**Error Responses:**
```
404 NOT_FOUND   → flight not found, or flight is CANCELLED
```

---

### Endpoint 4: GET /api/v1/flights

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `origin` | string | — | Filter by IATA origin code |
| `destination` | string | — | Filter by IATA destination code |
| `date` | string | — | Filter by departure date `YYYY-MM-DD` |
| `cabin` | string | — | `ECONOMY \| BUSINESS \| FIRST` |
| `airline` | string | — | Filter by airline name |
| `status` | string | all | `ACTIVE \| DELAYED \| CANCELLED` |
| `page` | number | 1 | |
| `limit` | number | 20 | Max 100 |
| `sortBy` | string | `createdAt` | `departureDate \| basePrice \| createdAt \| availableSeats` |
| `sortOrder` | string | `desc` | `asc \| desc` |

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flights retrieved successfully.",
  "data": {
    "flights": [ /* array of full flight objects including createdById */ ]
  },
  "meta": {
    "page":       1,
    "limit":      20,
    "total":      143,
    "totalPages": 8
  },
  "traceId": "tr-f47ac10b"
}
```

---

### Endpoint 5: DELETE /api/v1/flights/:id (Cancel Flight)

**Auth required:** Yes — `SUPER_ADMIN` only

**Request Body:** Empty `{}`

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight cancelled successfully.",
  "data": {
    "flightId": "abc123-def456-ghi789",
    "status":   "CANCELLED",
    "cancelledAt": "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
403 FORBIDDEN    → role is FLIGHT_ADMIN (not SUPER_ADMIN)
404 NOT_FOUND    → flight not found
400 ALREADY_CANCELLED → flight is already CANCELLED
```

---

### Endpoint 6: PATCH /internal/flights/:id/hold-seats

**Auth required:** No JWT check — internal network only (not proxied by Gateway)

**Request Body:**
```json
{
  "seats":     2,
  "bookingId": "booking-uuid-abc123"
}
```

**Success Response — 200 OK:**
```json
{
  "success":        true,
  "remainingSeats": 140,
  "heldUntil":      "2026-05-28T10:15:00.000Z"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR     → seats < 1 or seats > 9 or bookingId missing
404 NOT_FOUND            → flight not found
409 INSUFFICIENT_SEATS   → {
  "code": "INSUFFICIENT_SEATS",
  "message": "Not enough seats available",
  "availableSeats": 1,
  "requestedSeats": 2
}
400 FLIGHT_NOT_ACTIVE    → flight is CANCELLED or DELAYED with status that blocks booking
```

**Critical implementation note:**
The response `heldUntil` is `NOW() + 15 minutes` computed in the service. This value is used by the Booking Service to schedule the BullMQ seat-timeout job. The Flight Service does NOT create any timer or job itself — it only reports the expected expiry time.

---

### Endpoint 7: PATCH /internal/flights/:id/release-seats

**Auth required:** No JWT check — internal network only

**Request Body:**
```json
{
  "seats":     2,
  "bookingId": "booking-uuid-abc123"
}
```

**Success Response — 200 OK:**
```json
{
  "success":        true,
  "remainingSeats": 142
}
```

**Error Responses:**
```
400 VALIDATION_ERROR  → seats < 1 or bookingId missing
404 NOT_FOUND         → flight not found
```

**This endpoint is idempotent.** Calling it twice with the same payload is safe — `LEAST(available + seats, total)` ensures seats never exceed capacity.

---

### Endpoint 8: GET /health

**Auth required:** No

**Healthy Response — 200 OK:**
```json
{
  "status":    "healthy",
  "service":   "flight-service",
  "version":   "1.0.0",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database": "ok",
    "kafka":    "ok"
  }
}
```

Note: Flight Service does NOT use Redis — no Redis check here.

---

## 6. Zod Validation Schemas

These are the validation rules. Implement as Zod schemas in `src/routes/schemas/flight.schemas.ts`.

### CreateFlightSchema

| Field | Rule |
|---|---|
| `flightNumber` | string, min 3, max 20, regex `/^[A-Z0-9][A-Z0-9]-\d{1,4}[A-Z]?$/i`, transform toUpperCase |
| `airline` | string, min 2, max 100, trim |
| `origin` | string, length exactly 3, toUpperCase, regex `/^[A-Z]{3}$/` after transform |
| `destination` | string, length exactly 3, toUpperCase, regex `/^[A-Z]{3}$/` after transform |
| `departureDate` | string, regex `YYYY-MM-DD`, refine: date must be today or in the future |
| `departureTime` | string, regex `/^([01]\d\|2[0-3]):[0-5]\d$/` (HH:MM format) |
| `arrivalDate` | string, regex `YYYY-MM-DD` |
| `arrivalTime` | string, same regex as departureTime |
| `durationMinutes` | integer, min 1, max 1440 (24 hours) |
| `cabinClass` | enum: `ECONOMY \| BUSINESS \| FIRST` |
| `basePrice` | integer, min 1 (in paise — must be positive) |
| `totalSeats` | integer, min 1, max 600 |
| `availableSeats` | integer, min 0, max 600 |
| `aircraft` | optional, string, max 100, trim |
| `stops` | optional, integer, min 0, max 5, default 0 |
| `amenities` | optional, array of strings, max 20 items, each item max 50 chars |
| `baggageAllowance` | required object: `{ cabin: string, checked: string }` |
| `refundable` | optional, boolean, default false |

**Cross-field validations (`.refine()` or `.superRefine()`):**
- `origin !== destination` → error on `destination`: "Origin and destination cannot be the same"
- `availableSeats <= totalSeats` → error on `availableSeats`
- `(arrivalDate + arrivalTime) > (departureDate + departureTime)` → error on `arrivalTime`: "Arrival must be after departure"
- `durationMinutes` must match computed difference between departure and arrival (allow ±15 min tolerance)

### UpdateFlightSchema

Same fields as Create but all optional, with these differences:
- Disallow `flightNumber`, `origin`, `destination`, `departureDate`, `cabinClass` entirely — if present in body, return 400 with message "Flight identity fields cannot be updated"
- `status` is updatable but only allows `ACTIVE → DELAYED` or `DELAYED → ACTIVE` transitions (not to `CANCELLED` — use the cancel endpoint)
- Cross-field: if both `availableSeats` and `totalSeats` are provided, validate `availableSeats <= totalSeats`
- If only `availableSeats` is provided, validate against the current `totalSeats` (fetched from DB before update)

### HoldSeatsSchema

| Field | Rule |
|---|---|
| `seats` | integer, min 1, max 9 |
| `bookingId` | string (UUID), required |

### ReleaseSeatsSchema

Same as HoldSeatsSchema.

### ListFlightsQuerySchema

| Field | Rule |
|---|---|
| `origin` | optional, string, length 3, toUpperCase |
| `destination` | optional, string, length 3, toUpperCase |
| `date` | optional, string, regex `YYYY-MM-DD` |
| `cabin` | optional, enum: `ECONOMY \| BUSINESS \| FIRST` |
| `airline` | optional, string, max 100, trim |
| `status` | optional, enum: `ACTIVE \| DELAYED \| CANCELLED` |
| `page` | optional, coerce integer, min 1, default 1 |
| `limit` | optional, coerce integer, min 1, max 100, default 20 |
| `sortBy` | optional, enum: `departureDate \| basePrice \| createdAt \| availableSeats`, default `createdAt` |
| `sortOrder` | optional, enum: `asc \| desc`, default `desc` |

---

## 7. Kafka Event Publishing (Outbox Pattern)

### 7.1 Kafka Topic

**Topic:** `flight-inventory-events`

**Producer:** Flight Service (the only producer for this topic)

**Consumers:** Search Service (upserts MongoDB + invalidates Redis cache)

### 7.2 Standard Message Envelope

All Kafka messages use the cluster-wide envelope:

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

### 7.3 Event Payloads

**`FLIGHT_UPDATED`** — published on create AND update:
```json
{
  "flightId":        "abc123-def456-ghi789",
  "airline":         "IndiGo",
  "flightNumber":    "6E-204",
  "origin":          "DEL",
  "destination":     "BOM",
  "departureDate":   "2026-10-12",
  "departureTime":   "06:30",
  "arrivalDate":     "2026-10-12",
  "arrivalTime":     "09:15",
  "durationMinutes": 165,
  "cabinClass":      "ECONOMY",
  "basePrice":       499900,
  "availableSeats":  180,
  "totalSeats":      180,
  "aircraft":        "Airbus A320",
  "stops":           0,
  "amenities":       ["usb", "snack"],
  "baggageAllowance": { "cabin": "7kg", "checked": "15kg" },
  "refundable":      false,
  "status":          "ACTIVE"
}
```

**Why does `FLIGHT_UPDATED` include `availableSeats`?**
The Search Service uses this to initialise the MongoDB document with the correct seat count when a new flight is created. For updates, the Search Service replaces the entire document — simpler than partial updates.

**`SEATS_HELD`** — published on successful hold:
```json
{
  "flightId":       "abc123-def456-ghi789",
  "seatsHeld":      2,
  "remainingSeats": 140,
  "heldUntil":      "2026-05-28T10:15:00.000Z",
  "bookingId":      "booking-uuid-abc123"
}
```

**`SEATS_RELEASED`** — published on successful release:
```json
{
  "flightId":        "abc123-def456-ghi789",
  "seatsReleased":   2,
  "remainingSeats":  142,
  "bookingId":       "booking-uuid-abc123"
}
```

**`FLIGHT_CANCELLED`** — published when a flight is cancelled:
```json
{
  "flightId":      "abc123-def456-ghi789",
  "origin":        "DEL",
  "destination":   "BOM",
  "departureDate": "2026-10-12",
  "cabinClass":    "ECONOMY"
}
```

The Search Service removes the MongoDB document on receiving `FLIGHT_CANCELLED`. The Booking Service (Phase 4) would also consume this to initiate refund flows for confirmed bookings — but that is the Booking Service's responsibility, not the Flight Service's.

### 7.4 Outbox Worker Behaviour

The Outbox Worker is a `setInterval` loop running every 5 seconds inside the service process. It:

1. Queries `SELECT ... WHERE status='PENDING' ORDER BY created_at ASC LIMIT 100`
2. For each event: serializes as Kafka envelope using the event's `correlationId` (from the originating HTTP request's `X-Correlation-ID` header, stored in the outbox payload)
3. Uses `flightId` as the Kafka message key — this ensures all events for the same flight go to the same partition, preserving ordering (FLIGHT_UPDATED before SEATS_HELD for the same flight)
4. Marks `PUBLISHED` on success
5. Logs error and leaves `PENDING` on failure (retried next interval)

**Kafka Partition Key:**
```
Message key = flightId
```
This guarantees that events for flight `abc123` always go to partition N, and consumers process them in order. A `SEATS_HELD` event will never be processed before `FLIGHT_UPDATED` for the same flight.

---

## 8. Layered Architecture & File Map

```
services/flight-service/
│
├── prisma/
│   ├── schema.prisma              ← Flight + OutboxEvent models (Section 3.4)
│   ├── migrations/                ← Generated by prisma migrate dev
│   │   └── 20260528_init/
│   │       └── migration.sql      ← includes CHECK constraint SQL
│   └── seed.ts                    ← Seed a few sample ACTIVE flights for local dev
│
├── src/
│   │
│   ├── config/
│   │   ├── env.ts                 ← Zod-validated env vars — crash on startup if invalid
│   │   ├── database.ts            ← Prisma client singleton
│   │   ├── kafka.ts               ← KafkaJS producer instance (allowAutoTopicCreation: false)
│   │   └── logger.ts              ← Pino with AsyncLocalStorage for correlationId
│   │
│   ├── repositories/
│   │   ├── flight.repository.ts   ← All Prisma flight queries — no business logic
│   │   └── outbox.repository.ts   ← Insert/query/update outbox_events
│   │
│   ├── services/
│   │   ├── flight.service.ts      ← Business logic: create, update, cancel, list
│   │   └── inventory.service.ts   ← Seat hold/release with $transaction + FOR UPDATE
│   │
│   ├── controllers/
│   │   ├── flight.controller.ts   ← Public + admin HTTP handlers
│   │   └── internal.controller.ts ← Hold/release HTTP handlers (internal only)
│   │
│   ├── routes/
│   │   ├── flight.routes.ts       ← /api/v1/flights routes (public + admin)
│   │   ├── internal.routes.ts     ← /internal/flights routes (booking service)
│   │   ├── health.routes.ts       ← GET /health
│   │   ├── metrics.routes.ts      ← GET /metrics
│   │   └── schemas/
│   │       └── flight.schemas.ts  ← All Zod schemas (Section 6)
│   │
│   ├── middlewares/
│   │   ├── requireRole.ts         ← Reads X-User-Role header, enforces minimum role
│   │   ├── validate.ts            ← req.body Zod validation
│   │   ├── validateQuery.ts       ← req.query Zod validation (list endpoint)
│   │   ├── validateParams.ts      ← req.params Zod validation (UUID check)
│   │   └── errorHandler.ts        ← Global Express error handler
│   │
│   ├── events/
│   │   ├── producers/
│   │   │   └── flight.producer.ts ← Serializes + publishes Kafka envelope
│   │   └── outbox.worker.ts       ← setInterval: polls outbox → publishes → marks done
│   │
│   ├── types/
│   │   └── express.d.ts           ← Augments req: userId?, userRole?, validatedQuery?
│   │
│   ├── utils/
│   │   └── response.utils.ts      ← sendSuccess(), sendError() — same pattern as other services
│   │
│   ├── app.ts                     ← Express setup: helmet, routes, error handler
│   └── server.ts                  ← Bootstrap: DB connect, Kafka connect, start outbox worker
│
├── tests/
│   ├── unit/
│   │   ├── services/
│   │   │   ├── flight.service.test.ts
│   │   │   └── inventory.service.test.ts   ← seat hold/release logic
│   │   └── middlewares/
│   │       └── requireRole.test.ts
│   └── integration/
│       ├── flight.create.test.ts
│       ├── flight.update.test.ts
│       ├── flight.hold.test.ts             ← Critical: concurrent hold tests
│       └── flight.release.test.ts
│
├── .env.example
├── package.json
└── tsconfig.json
```

### Layer Rules

```
Routes         → middleware validation → controller
Controller     → calls service (never repository or Prisma directly)
Service        → calls repository, writes to outbox in same transaction
Repository     → Prisma queries only — no if/else, no calculations
Internal ctrl  → calls inventory.service → same layer rules

Outbox Worker  → runs independently, reads outbox → publishes Kafka → updates status
```

### Key Design Decisions

**`inventory.service.ts` is separate from `flight.service.ts`** because it has a fundamentally different transaction pattern. The `flight.service.ts` does standard CRUD (begin → write → commit). The `inventory.service.ts` must use `prisma.$queryRaw` for `SELECT ... FOR UPDATE` — Prisma's typed query builder does not support `FOR UPDATE`. Keeping these separate prevents mixing the locking logic into general flight CRUD.

**`flight.producer.ts` is called by `outbox.worker.ts`**, not by services directly. Services only write to the outbox table. The outbox worker is the only code that touches Kafka. This strict separation means: if Kafka is down during a flight create, the HTTP response still returns 201 (DB write succeeded), and the event will be published when Kafka recovers.

**`requireRole.ts` middleware** reads `X-User-Id` and `X-User-Role` headers, validates they are present, and checks the role. Attaches `req.userId` and `req.userRole` for use by controllers. The `X-User-Id` is stored as `createdById` when creating a flight — this links the flight back to the admin who created it.

---

## 9. npm Dependencies

**File: `services/flight-service/package.json`**

```json
{
  "name": "@skyhub/flight-service",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev":          "tsx watch src/server.ts",
    "build":        "tsc --project tsconfig.json",
    "start":        "node dist/server.js",
    "migrate":      "prisma migrate deploy",
    "migrate:dev":  "prisma migrate dev",
    "seed":         "tsx prisma/seed.ts",
    "lint":         "eslint .",
    "test":         "vitest",
    "test:coverage":"vitest run --coverage",
    "typecheck":    "tsc --noEmit"
  },
  "dependencies": {
    "@prisma/client":       "^5.14.0",
    "@skyhub/common-utils": "*",
    "@skyhub/shared-types": "*",
    "cors":                 "^2.8.5",
    "dotenv":               "^16.4.5",
    "express":              "^5.2.1",
    "helmet":               "^7.1.0",
    "kafkajs":              "^2.2.4",
    "pino":                 "^9.2.0",
    "pino-http":            "^10.2.0",
    "prom-client":          "^15.1.2",
    "uuid":                 "^9.0.1",
    "zod":                  "^3.23.8"
  },
  "devDependencies": {
    "@types/cors":          "^2.8.17",
    "@types/express":       "^5.0.6",
    "@types/node":          "^22.0.0",
    "@types/supertest":     "^6.0.2",
    "@vitest/coverage-v8":  "^1.6.0",
    "pino-pretty":          "^11.0.0",
    "prisma":               "^5.14.0",
    "supertest":            "^6.3.4",
    "tsx":                  "^4.15.7",
    "vitest":               "^1.6.0"
  }
}
```

### Dependency Explanations

| Package | Why |
|---|---|
| `@prisma/client` | Type-safe PostgreSQL ORM. Handles parameterized queries (prevents SQL injection). For `SELECT...FOR UPDATE`, use `prisma.$queryRaw` with tagged template literals (also parameterized). |
| `kafkajs` | Producer only — publishes `flight-inventory-events`. No consumer in this service. |
| `prom-client` | Exposes `/metrics`. Custom counter for `flight_hold_requests_total{result}` and gauge for `outbox_pending_count`. |
| `uuid` | Generates `eventId` for Kafka message envelopes. |
| No `ioredis` | Flight Service does NOT use Redis directly. It publishes Kafka events; the Search Service handles Redis cache invalidation. |
| No `bcrypt` | No passwords. No authentication logic. |
| No `jose` | No JWT signing. Relies on Gateway for JWT verification — reads trusted headers only. |

---

## 10. Environment Variables

**File: `services/flight-service/.env.example`**

```bash
# ── Server ────────────────────────────────────────────────────────────
NODE_ENV=development
PORT=3002
SERVICE_NAME=flight-service

# ── Database (PostgreSQL via Prisma) ─────────────────────────────────
# connection_limit: max Prisma pool connections (keep ≤ 10 for local dev)
# pool_timeout: seconds to wait for a free connection before erroring
DATABASE_URL=postgresql://skyhub:skyhub_local@localhost:5432/skyhub_flight_db?connection_limit=10&pool_timeout=10

# ── Kafka ─────────────────────────────────────────────────────────────
# KAFKA_BROKERS: comma-separated list for multi-broker production clusters
# e.g., KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=flight-service
KAFKA_TOPIC_FLIGHT_EVENTS=flight-inventory-events

# ── Outbox Worker ─────────────────────────────────────────────────────
# Interval in milliseconds to poll outbox_events for pending publications
# 5000ms = 5 seconds (matches cluster-wide standard)
OUTBOX_POLL_INTERVAL_MS=5000

# ── Seat Hold ─────────────────────────────────────────────────────────
# How many minutes a seat hold lasts (reported in hold-seats response)
# Booking Service uses this to schedule the BullMQ seat-timeout job
# Must match the BullMQ job delay configured in Booking Service
SEAT_HOLD_DURATION_MINUTES=15

# ── Observability ─────────────────────────────────────────────────────
LOG_LEVEL=info
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

### Env Validation Schema (Zod) — Key Rules

Implement in `src/config/env.ts`. Crash-fast if any required variable is missing.

| Variable | Rule |
|---|---|
| `NODE_ENV` | enum: `development \| production \| test` |
| `PORT` | string → transform to number, default `3002` |
| `DATABASE_URL` | `z.string().url()` — Prisma URL format |
| `KAFKA_BROKERS` | `z.string()` — comma-separated, parsed in kafka.ts with `.split(',')` |
| `KAFKA_CLIENT_ID` | `z.string()` |
| `KAFKA_TOPIC_FLIGHT_EVENTS` | `z.string()` |
| `OUTBOX_POLL_INTERVAL_MS` | string → transform to number, default `5000` |
| `SEAT_HOLD_DURATION_MINUTES` | string → transform to number, default `15` |
| `LOG_LEVEL` | enum: `error \| warn \| info \| debug`, default `info` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `z.string().url().optional()` |

---

## 11. Step-by-Step Build Plan

Work through each step in order. Validate each before proceeding.

---

### Step 1: Project Setup & Tooling

1. Create `services/flight-service/` directory
2. Create `package.json` from Section 9
3. Create `tsconfig.json`:
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "outDir": "./dist", "rootDir": "./src" },
  "include": ["src/**/*", "prisma/seed.ts"],
  "references": [
    { "path": "../../packages/shared-types" },
    { "path": "../../packages/common-utils" }
  ]
}
```
4. Run `npm install` from monorepo root
5. Create `src/config/env.ts` with Zod schema (Section 10)
6. Copy `.env.example` to `.env`, fill in values

**Validation:** `npm run typecheck` → zero errors. `npm run dev` → service should crash with Zod error if any env var is missing, else show "port listening" log.

---

### Step 2: Database Migration

1. Ensure Postgres is running: `docker compose up -d`
2. Verify `skyhub_flight_db` database exists (created by `scripts/init-databases.sql`)
3. Initialize Prisma: `cd services/flight-service && npx prisma init`
4. Replace generated `schema.prisma` with the schema from Section 3.4
5. Run initial migration: `npx prisma migrate dev --name init`
6. After migration, manually add CHECK constraints by editing the generated `migration.sql`:
   - Add the 4 CHECK constraints from Section 3.3 to the migration file before running, or add them in a second migration
7. Verify with Prisma Studio: `npx prisma studio` — confirm `flights` and `outbox_events` tables exist with all columns

**Seed file (`prisma/seed.ts`) purpose:** Create 3–5 sample ACTIVE flights for local development so the Search Service has data to consume immediately. Include flights across different routes and cabin classes.

Seed structure:
```typescript
// seed.ts — load dotenv/config first (same as User Service pattern)
// Create sample flights on future dates using prisma.flight.createMany([...])
// Skip if flights already exist (use upsert with flightNumber+departureDate+cabinClass)
```

**Validation:** Run `npm run seed`. `SELECT COUNT(*) FROM flights;` should return 5 (or however many you seed).

---

### Step 3: Utilities & Common Infrastructure

1. Create `src/config/database.ts` — Prisma client singleton (same pattern as User Service)
2. Create `src/config/kafka.ts` — KafkaJS producer with `allowAutoTopicCreation: false`
3. Create `src/config/logger.ts` — Pino with AsyncLocalStorage (same pattern as User Service)
4. Create `src/utils/response.utils.ts` — `sendSuccess()`, `sendError()` (copy from User Service pattern)
5. Create `src/types/express.d.ts` — augment `Request` with `userId?`, `userRole?`, `validatedQuery?`, `validatedParams?`
6. Create `src/middlewares/requireRole.ts` — reads `X-User-Id` + `X-User-Role` headers, enforces role

**`requireRole` middleware signature:**
```typescript
// Returns middleware that enforces one of the provided roles
export function requireRole(...allowedRoles: string[]): RequestHandler
// Usage:
// router.post('/', requireRole('FLIGHT_ADMIN', 'SUPER_ADMIN'), controller.create)
// router.delete('/:id', requireRole('SUPER_ADMIN'), controller.cancel)
```

7. Create `src/middlewares/validate.ts`, `validateQuery.ts`, `validateParams.ts` (same pattern as Search Service)
8. Create `src/middlewares/errorHandler.ts` — same global error handler pattern (checks `AppError`, logs unexpecteds)
9. Create `src/routes/schemas/flight.schemas.ts` from Section 6

**Validation:** `npm run typecheck` → zero errors.

---

### Step 4: Repository Layer

Create `src/repositories/flight.repository.ts` with these methods:

```
findById(id: string): Promise<Flight | null>
findAll(filters: FlightListFilters): Promise<{ flights: Flight[], total: number }>
create(data: CreateFlightData): Promise<Flight>
update(id: string, data: UpdateFlightData): Promise<Flight>
cancel(id: string): Promise<Flight>
checkDuplicate(flightNumber, departureDate, cabinClass): Promise<boolean>
```

Create `src/repositories/outbox.repository.ts`:
```
create(tx: PrismaTransaction, eventType: string, payload: object): Promise<void>
getPending(limit: number): Promise<OutboxEvent[]>
markPublished(id: string): Promise<void>
markFailed(id: string): Promise<void>
```

**Important:** The `outbox.repository.ts` `create` method accepts a Prisma transaction (`tx`) as its first argument. This allows the service layer to call `outboxRepository.create(tx, ...)` inside a `prisma.$transaction()` block, ensuring atomicity.

**`findAll` pagination pattern:**
```typescript
const [flights, total] = await prisma.$transaction([
  prisma.flight.findMany({ where, skip, take, orderBy }),
  prisma.flight.count({ where }),
]);
return { flights, total };
```

**Validation:** Write a quick test that calls `flightRepository.create(...)` and checks Prisma Studio.

---

### Step 5: Service Layer

**`src/services/flight.service.ts`** — implements Features 1–5:

Key business rules to enforce in the service (not repository, not controller):
- Check for duplicate `(flightNumber, departureDate, cabinClass)` before create
- Validate `origin !== destination`
- Validate `departureDate` is not in the past (on create only — past dates are ok for historical records if updating)
- Validate `availableSeats <= totalSeats`
- Wrap DB write + outbox insert in `prisma.$transaction()`
- For update: fetch current `totalSeats` from DB if only `availableSeats` is being updated (needed for cross-field validation)
- For cancel: check current status before attempting cancel

**`src/services/inventory.service.ts`** — implements Features 6–7:

This is the only place in the codebase that uses `SELECT ... FOR UPDATE`. The Prisma ORM does not support `FOR UPDATE` natively in its typed query builder, so use `prisma.$queryRaw`:

```
Key algorithm for holdSeats():
  prisma.$transaction(async (tx) => {
    1. Raw query: SELECT id, available_seats FROM flights WHERE id = $flightId FOR UPDATE
       This acquires an exclusive row lock. Concurrent hold requests BLOCK here.
    2. If flight not found → throw 404
    3. If flight.available_seats < seats → throw 409 with available count
    4. If flight.status !== 'ACTIVE' → throw 400 FLIGHT_NOT_ACTIVE
    5. tx.flight.update({ where: { id }, data: { availableSeats: { decrement: seats } } })
    6. outboxRepository.create(tx, 'SEATS_HELD', { flightId, seatsHeld, remainingSeats, heldUntil, bookingId })
    7. return { remainingSeats: updated.availableSeats, heldUntil }
  })

Key algorithm for releaseSeats():
  prisma.$transaction(async (tx) => {
    1. tx.flight.findUnique({ where: { id: flightId } }) → 404 if null
    2. tx.$executeRaw`UPDATE flights
         SET available_seats = LEAST(available_seats + ${seats}, total_seats)
         WHERE id = ${flightId}`
    3. tx.flight.findUnique({ where: { id: flightId } }) → get updated available_seats
    4. outboxRepository.create(tx, 'SEATS_RELEASED', { flightId, seatsReleased: seats, remainingSeats, bookingId })
    5. return { remainingSeats }
  })
```

**Validation:** Start the service. Call the create endpoint from Postman/cURL. Verify row appears in DB and outbox. Verify calling hold-seats endpoint decrements `available_seats`.

---

### Step 6: Controllers + Routes

**`src/controllers/flight.controller.ts`** — handlers for all public/admin endpoints:
```
create(req, res)         → validate → flightService.create() → 201
update(req, res)         → validate → flightService.update() → 200
getById(req, res)        → flightService.getById() → 200
list(req, res)           → flightService.list() → 200 + meta
cancel(req, res)         → flightService.cancel() → 200
```

**`src/controllers/internal.controller.ts`** — handlers for seat operations:
```
holdSeats(req, res)      → validate → inventoryService.holdSeats() → 200
releaseSeats(req, res)   → validate → inventoryService.releaseSeats() → 200
```

**`src/routes/flight.routes.ts`:**
```
GET    /                  → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → list
POST   /                  → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → create
GET    /:id               → (no auth) → getById
PATCH  /:id               → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → update
DELETE /:id               → requireRole(SUPER_ADMIN) → cancel
```

**`src/routes/internal.routes.ts`:**
```
PATCH  /:id/hold-seats    → (no auth check) → holdSeats
PATCH  /:id/release-seats → (no auth check) → releaseSeats
```

**`src/app.ts` route mounting:**
```typescript
app.use('/api/v1/flights', flightRouter)       // proxied by Gateway
app.use('/internal/flights', internalRouter)   // NOT proxied — internal only
app.use('/', healthRouter)
app.use('/', metricsRouter)
app.use(globalErrorHandler)                    // must be last
```

**Validation:** Full CRUD via Postman. Create a flight → PATCH it → GET it → cancel it. Verify 403 when sending with wrong role header.

---

### Step 7: Outbox Worker + Kafka Producer

1. Create `src/events/producers/flight.producer.ts`:
   - Takes `OutboxEvent` from the DB
   - Builds the standard Kafka envelope
   - Publishes with `flightId` as the message key (from `payload.flightId`)
   - Returns success/failure boolean

2. Create `src/events/outbox.worker.ts`:
   - Exported function `startOutboxWorker()` that starts a `setInterval`
   - Reads `env.OUTBOX_POLL_INTERVAL_MS` for the interval
   - Calls `outboxRepository.getPending(100)` each tick
   - For each event: calls `flightProducer.publish(event)` → marks PUBLISHED or FAILED
   - Logs at `info` level: `{ eventType, flightId, outboxId }` on success
   - Logs at `error` level on failure (does not rethrow — worker must survive errors)

3. Update `src/server.ts` to call `startOutboxWorker()` after Kafka producer connects

**End-to-end test:**
- Create a flight via POST API
- Verify row appears in `outbox_events` with `status='PENDING'`
- Wait 5 seconds (one outbox poll cycle)
- Verify `status` is now `'PUBLISHED'`
- Use Kafka console consumer or Kafka UI to verify the message landed in `flight-inventory-events`

---

### Step 8: Health, Metrics, server.ts

1. Create `src/routes/health.routes.ts` — checks Prisma `$queryRaw SELECT 1` and Kafka producer status
2. Create `src/routes/metrics.routes.ts` — `collectDefaultMetrics()` + custom flight hold counter + outbox gauge
3. Finalise `src/server.ts`:

```
bootstrap():
  1. await prisma.$connect()
  2. await kafkaProducer.connect()
  3. startOutboxWorker()
  4. const server = app.listen(env.PORT)
  5. process.on('SIGTERM', shutdown)
  6. process.on('SIGINT', shutdown)

shutdown(signal):
  1. server.close()
  2. kafkaProducer.disconnect()  ← flushes pending Kafka messages
  3. prisma.$disconnect()        ← closes connection pool
  4. process.exit(0)
```

**Full validation:**
```bash
curl http://localhost:3002/health
# Expected: { "status": "healthy", "checks": { "database": "ok", "kafka": "ok" } }

curl http://localhost:3002/metrics
# Expected: Prometheus text with flight_hold_requests_total, outbox_pending_count

# Test internal endpoint
curl -X PATCH http://localhost:3002/internal/flights/{id}/hold-seats \
  -H "Content-Type: application/json" \
  -d '{"seats": 2, "bookingId": "test-booking-id"}'
# Expected: { "success": true, "remainingSeats": 178, "heldUntil": "..." }
```

---

## 12. Testing Strategy

### Unit Tests

**`tests/unit/services/inventory.service.test.ts`** — the most important test file:

Mock Prisma client using `vitest.mock()`. Test the logic of `holdSeats` and `releaseSeats` without a real database:

| Test Case | What to Verify |
|---|---|
| Hold succeeds with enough seats | `availableSeats` decremented, `SEATS_HELD` outbox event created |
| Hold fails with insufficient seats | 409 error thrown with `availableSeats` in error detail |
| Hold on non-existent flight | 404 error thrown |
| Hold on CANCELLED flight | 400 FLIGHT_NOT_ACTIVE error |
| Release increments seats correctly | `availableSeats` back to original value |
| Release capped at `totalSeats` | `LEAST(available + n, total)` — never exceeds capacity |
| Release on non-existent flight | 404 error |

**`tests/unit/services/flight.service.test.ts`:**

| Test Case | What to Verify |
|---|---|
| Create with duplicate identity | 409 conflict |
| Create with origin === destination | 422 business rule violation |
| Create with past departure date | 422 business rule violation |
| Create with availableSeats > totalSeats | 422 business rule violation |
| Update disallowed field (origin) | 400 validation error |
| Update CANCELLED flight | 400 error |
| Cancel already-cancelled flight | 400 error |

**`tests/unit/middlewares/requireRole.test.ts`:**

| Test Case | What to Verify |
|---|---|
| Missing X-User-Role header | 401 Unauthorized |
| Correct role (FLIGHT_ADMIN) | calls next() |
| Insufficient role (CUSTOMER → FLIGHT_ADMIN required) | 403 Forbidden |
| SUPER_ADMIN can access FLIGHT_ADMIN route | calls next() |

### Integration Tests

Use a real PostgreSQL test database (spin up with Docker or use a separate `skyhub_flight_test_db`). Use `beforeEach` to truncate tables.

**`tests/integration/flight.create.test.ts`:**
```
POST /api/v1/flights

✅ Creates flight with all required fields
✅ Returns 201 with full flight object
✅ Outbox event is in DB with status PENDING
✅ 409 on duplicate (flightNumber + date + cabin)
✅ 400 VALIDATION_ERROR for invalid IATA code
✅ 422 for origin === destination
✅ 403 when X-User-Role is CUSTOMER
✅ 401 when X-User-Role header is missing
```

**`tests/integration/flight.hold.test.ts`** — critical concurrency tests:
```
PATCH /internal/flights/:id/hold-seats

✅ Single hold succeeds, decrements available_seats
✅ Returns correct remainingSeats and heldUntil
✅ 409 when requesting more seats than available
✅ Outbox event created with status PENDING

Concurrency test (the most important test in this file):
  Setup: Flight with 2 available seats
  Action: fire 3 concurrent hold requests each requesting 2 seats using Promise.all
  Assert:
    - Exactly 1 request succeeds (200) with remainingSeats = 0
    - Remaining 2 requests fail (409 INSUFFICIENT_SEATS)
    - Final available_seats in DB = 0 (not negative)

  Implementation hint:
    const results = await Promise.all([
      request.patch(`/internal/flights/${id}/hold-seats`).send({ seats: 2, bookingId: 'b1' }),
      request.patch(`/internal/flights/${id}/hold-seats`).send({ seats: 2, bookingId: 'b2' }),
      request.patch(`/internal/flights/${id}/hold-seats`).send({ seats: 2, bookingId: 'b3' }),
    ]);
    const successes = results.filter(r => r.status === 200);
    const failures  = results.filter(r => r.status === 409);
    expect(successes).toHaveLength(1);
    expect(failures).toHaveLength(2);
```

**The concurrency test is what proves the `SELECT ... FOR UPDATE` works correctly.** Without the row lock, all 3 requests would see `availableSeats = 2`, all decrement to 0, and the DB would end up at -4 (or a constraint violation if you have the CHECK constraint).

### Test Coverage Targets

| Layer | Target | Focus |
|---|---|---|
| `inventory.service` | 100% | Seat hold/release logic — this is the most critical code |
| `flight.service` | > 90% | All validation branches, transaction success/failure |
| `requireRole` middleware | 100% | Every role combination |
| Integration: create/update | > 80% | Happy path + key error cases |
| Integration: hold/release + concurrency | > 90% | Correctness of locking under concurrency |

### Local Test Run

```bash
# All tests
npm run test

# Watch mode during development
npm run test -- --watch

# Run only concurrency tests
npm run test -- tests/integration/flight.hold.test.ts

# Coverage report
npm run test:coverage
```

---

> **This document is the complete build specification for the SkyHub Flight Service.** Every feature, design decision, database model, API endpoint, Kafka event, and test case required to build this service from scratch is documented here. The seat hold mechanism (Section 2 Feature 6 and Section 11 Step 5) is the most critical part — read it carefully before implementing `inventory.service.ts`.

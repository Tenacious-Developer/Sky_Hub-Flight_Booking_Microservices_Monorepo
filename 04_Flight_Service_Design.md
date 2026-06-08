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
ADMIN WRITE PATH (admin creates a flight schedule + instance)

ADMIN CLIENT
  └── POST /api/v1/flights/schedules ─────────── API GATEWAY
                                1. Verify JWT
                                2. Check X-User-Role = FLIGHT_ADMIN | SUPER_ADMIN
                                3. Proxy to FLIGHT SERVICE
                                               │
                                    FLIGHT SERVICE (Port 3002)
                                4. Zod validate body
                                5. BEGIN TRANSACTION
                                     INSERT INTO flight_schedules (...)
                                     INSERT INTO outbox_events (FLIGHT_UPDATED, ...)
                                   COMMIT
                                6. Return 201 Created

  └── POST /api/v1/flights/instances ──────────── API GATEWAY → FLIGHT SERVICE
                                1. Verify schedule exists
                                2. BEGIN TRANSACTION
                                     INSERT INTO flight_instances (...)
                                     INSERT INTO seat_inventories (one per cabin)
                                     INSERT INTO outbox_events (FLIGHT_UPDATED, ...)
                                   COMMIT
                                3. Return 201 Created

                       (Background: Outbox Worker)
                                4. Publish FLIGHT_UPDATED → Kafka: flight-inventory-events
                                5. Mark outbox event PUBLISHED

  ✅ Admin sees: 201 immediately
  ✅ Search Service: receives Kafka event → upserts MongoDB → invalidates Redis cache

─────────────────────────────────────────────────────────────────────

INTERNAL SEAT HOLD PATH (Booking Service holds seats before payment)

BOOKING SERVICE (internal network only — never via Gateway)
  └── PATCH /internal/flights/instances/:id/hold-seats { seats: 2, cabinClass: "ECONOMY", fareClass: "Y", bookingId: "..." }
                                               │
                                    FLIGHT SERVICE
                                1. BEGIN TRANSACTION
                                     SELECT ... FROM seat_inventories WHERE flight_instance_id = ? AND cabin_class = ? AND fare_class = ? FOR UPDATE
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
| Airport & aircraft reference data (catalog) | Booking records |
| Flight catalog (schedules, instances, inventories) | Payment records |
| Seat inventory write operations | Search index (Search Service owns it) |
| `flight-inventory-events` Kafka topic (producer only) | JWT signing or user identity |
| Internal hold/release endpoints | Loyalty tier management |

**Data contract with other services:**
- **Search Service** receives flight data exclusively via Kafka events (`FLIGHT_UPDATED`, `SEATS_HELD`, `SEATS_RELEASED`, `FLIGHT_CANCELLED`)
- **Booking Service** calls Flight Service synchronously via HTTP internal endpoints (`/internal/flights/instances/:id/hold-seats`, `/internal/flights/instances/:id/release-seats`) — these are the ONLY cross-service HTTP calls Flight Service receives
- **API Gateway** proxies public read + admin write routes to Flight Service

---

## 2. Complete Feature List

### Feature 1: Create Flight Schedule (Admin)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**What a FlightSchedule is:** The static template for a recurring route — `6E-204 DEL→BOM at 06:30`. It does not represent a specific date; it is shared by all future instances of that route.

**Flow:**
1. Zod validates the full schedule body (see Section 6)
2. Extract `createdById` from `req.userId` (set by `requireRole` middleware from `X-User-Id` header)
3. Validate `originCode !== destinationCode` → 422 if same
4. Verify `originCode` exists in `airports` table → 404 if not found
5. Verify `destinationCode` exists in `airports` table → 404 if not found
6. Verify `aircraftId` exists in `aircrafts` table → 404 if not found
7. Check for duplicate `(flightNumber, originCode, destinationCode)` → 409 if already exists
8. `INSERT INTO flight_schedules (...)` including `createdById` — no outbox needed here (the schedule itself is not a Search Service event; only instances with inventory trigger search updates)
9. Return `201 Created` with the new schedule

---

### Feature 2: Create Flight Instance (Admin)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**What a FlightInstance is:** A specific dated operational flight — `6E-204 on 2026-10-12`. Tied to a schedule and contains one or more `SeatInventory` buckets (one per cabin class).

**Flow:**
1. Zod validates the body (see Section 6)
2. Verify `scheduleId` exists → 404 if not found
3. Validate `departureDate` is today or in the future → 422 if in the past
4. Validate `arrivalDate >= departureDate` → 422 if invalid
5. Check for duplicate `(scheduleId, departureDate)` → 409 if instance already scheduled
6. Validate inventories array has no duplicate `(cabinClass, fareClass)` pairs.
   - Fetch the associated `FlightSchedule` (including the `Aircraft`).
   - Validate that the sum of `totalSeats` across all items in the `inventories` array does not exceed the aircraft's `totalCapacity` (`sum(totalSeats) <= aircraft.totalCapacity`) → 422 if it exceeds capacity.
7. In ONE atomic DB transaction:
   - `INSERT INTO flight_instances (...)` with status `SCHEDULED`
   - `INSERT INTO seat_inventories (...)` for each inventory item in the request
   - `INSERT INTO outbox_events (event_type='FLIGHT_UPDATED', payload={full denormalized flight per inventory bucket})`
     — one outbox row per `SeatInventory` bucket, each keyed by `inventoryId`
8. Return `201 Created` with the full instance + inventory detail

---

### Feature 3: Update Flight Instance Operational Data (Admin)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**What is updatable via this endpoint (operational state only):**
- `status` — `SCHEDULED | BOARDING | DEPARTED | DELAYED` (setting `CANCELLED` is rejected — use the dedicated cancel endpoint)
- `gate` — gate assignment
- `actualDepartureTime` — real departure time once known
- `actualArrivalTime` — real arrival time once known

**What is NOT updatable via this endpoint:**
- `scheduleId`, `departureDate`, `arrivalDate` — define the flight identity; cancel and recreate if these need to change
- Pricing and seat counts — use the dedicated inventory update endpoint (Feature 4)

**Flow:**
1. Zod validates partial body — `CANCELLED` is excluded from the status enum so Zod returns 400 before this request ever reaches the service layer
2. Fetch `FlightInstance` by ID → 404 if not found
3. In ONE transaction:
   - `UPDATE flight_instances SET ...`
   - If operational `status` has changed (e.g. to BOARDING, DEPARTED, or DELAYED): `INSERT INTO outbox_events (FLIGHT_UPDATED, {full denormalized payload})` for each seat inventory bucket of the flight instance. This notifies the Search Service to sync the flight's operational status. (Other operational fields like `gate` and `actualDepartureTime` do not trigger outbox events).
4. Return `200 OK` with the full updated instance shape

---

### Feature 4: Update Seat Inventory (Admin)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**What is updatable:**
- `basePrice` — price change (e.g., promotional discount, yield management)
- `totalSeats` — capacity change (e.g., aircraft swapped to a larger plane)
- `availableSeats` — manual correction (e.g., ops team adjustment)
- `baggageAllowance` — policy change
- `refundable` — fare policy change

**Rules:**
- `availableSeats` cannot exceed `totalSeats` → 422 if violated
- `availableSeats` cannot go below 0 → 422 if violated
- Changing `basePrice` publishes a `FLIGHT_UPDATED` Kafka event so the Search Service reflects the new price
- Changing `totalSeats` also triggers `FLIGHT_UPDATED`

**Flow:**
1. Fetch `FlightInstance` by `instanceId` (including its `schedule` and all its `inventories`) → 404 if not found
2. Fetch `SeatInventory` by `inventoryId` → 404 if not found; verify it belongs to the instance → 403 if not
3. If `totalSeats` is being updated:
   - Fetch the associated `Aircraft` capacity via `schedule.aircraftId`.
   - Calculate the new proposed sum of `totalSeats` across all inventories of the flight instance (replacing the old value of the targeted inventory with the new value).
   - Validate that `proposed_sum <= aircraft.totalCapacity` → 422 if it exceeds capacity.
4. In ONE transaction:
   - `UPDATE seat_inventories SET ...`
   - If `basePrice` or `totalSeats` or `availableSeats` changed: `INSERT INTO outbox_events (FLIGHT_UPDATED, {full denormalized payload})`
5. Return `200 OK` with the updated inventory

---

### Feature 5: Get Flight Instance by ID (Public)

**Who can call:** Anyone — no auth required

**Flow:**
1. Fetch `FlightInstance` by `id`, including its `schedule` and `inventories`
2. If `status === 'CANCELLED'` → return 404 (treat cancelled flights as non-existent for public consumers)
3. Return full flight detail with schedule and all inventory buckets

---

### Feature 6: List Flight Instances (Admin Only)

**Who can call:** `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Purpose:** Admin panel flight management — shows ALL instances including `CANCELLED` and `DELAYED`.

**Supported filters:**
- `originCode`, `destinationCode` — IATA code filter
- `date` — departure date filter (YYYY-MM-DD)
- `cabinClass` — `ECONOMY | BUSINESS | FIRST`
- `airline` — substring match (via schedule join)
- `status` — `SCHEDULED | BOARDING | DEPARTED | DELAYED | CANCELLED`
- `page`, `limit` (default: page=1, limit=20, max=100)
- `sortBy` — `DEPARTURE_DATE | CREATED_AT` (default: `CREATED_AT`)
- `sortOrder` — `ASC | DESC` (default: `DESC`)

---

### Feature 7: Cancel Flight Instance (Admin)

**Who can call:** `SUPER_ADMIN` only (cancellation is irreversible — higher privilege required)

**Flow:**
1. Fetch instance by ID → 404 if not found
2. Check `status !== 'CANCELLED'` → 400 if already cancelled
3. In ONE transaction:
   - `UPDATE flight_instances SET status = 'CANCELLED', cancelled_at = NOW() WHERE id = {id}`
   - `UPDATE seat_inventories SET available_seats = 0 WHERE flight_instance_id = {id}`
   - For each `SeatInventory` row: `INSERT INTO outbox_events (FLIGHT_CANCELLED, { inventoryId, instanceId, origin, destination, departureDate })`
   - One outbox row per inventory bucket so the Search Service can delete each MongoDB document by `inventoryId`
4. Return `200 OK`

**Booking Service consideration:**
Cancelling a flight with confirmed bookings triggers compensating transactions. The Flight Service only publishes `FLIGHT_CANCELLED`. The Booking Service (Phase 4) consumes this event to initiate refund sagas.

---

### Feature 8: Hold Seats (Internal — Booking Service Only)

**Who can call:** Only Booking Service, via internal HTTP (not proxied by Gateway)

**Why this is the most critical endpoint:**
ACID guarantee — two users requesting the last 2 seats at the same millisecond must result in exactly one success and one 409. This requires a database-level exclusive row lock on the `seat_inventories` row.

**Concurrency & Isolation Level:**
- **Isolation Level:** `Read Committed` (default for PostgreSQL). Under this isolation level, concurrent transactions block on the `FOR UPDATE` query rather than throwing serialization errors. Once the locking transaction commits, the waiting transaction resumes, reads the *committed* updated value (preventing Lost Updates), and executes correctly.
- **Lock Scope:** Exclusive row lock (`FOR UPDATE`) on the specific `seat_inventories` record. Since the query uses the unique compound index `uq_inventory_bucket` (`(flightInstanceId, cabinClass, fareClass)`), it performs an index scan locking only the target row, preventing deadlock escalation.

**Flow using `SELECT ... FOR UPDATE`:**
```
1. Booking Service sends:
   PATCH /internal/flights/instances/{instanceId}/hold-seats
   Body: { seats: 2, cabinClass: "ECONOMY", fareClass: "Y", bookingId: "f47ac10b-58cc-4372-a567-0e02b2c3d479" }

2. Flight Service executes (inside Prisma $transaction):

   Step A: Idempotency Guard
   SELECT payload FROM outbox_events WHERE event_type = 'SEATS_HELD' AND payload->>'bookingId' = {bookingId}
   IF found → return 200 with stored remainingSeats & heldUntil (no updates performed)

   Step B: Find inventory bucket and acquire row lock
   SELECT id, available_seats, total_seats
   FROM seat_inventories
    WHERE flight_instance_id = {instanceId}
      AND cabin_class = {cabinClass}::"CabinClass"  ← Explicitly cast string to Postgres enum type
      AND fare_class = {fareClass}
   FOR UPDATE  ← PostgreSQL acquires exclusive lock on this specific row
                  All other concurrent SELECT...FOR UPDATE on this row WAIT
                  until this transaction commits or rolls back

   Step C: Check flight instance is bookable
   SELECT status FROM flight_instances WHERE id = {instanceId}
   IF status !== 'SCHEDULED' → ROLLBACK → return 400 FLIGHT_NOT_ACTIVE

   Step D: Check availability
   IF inventory.available_seats < seats:
     ROLLBACK (lock released)
     Return 409 { code: 'INSUFFICIENT_SEATS', availableSeats: inventory.available_seats }

   Step E: Decrement seats + write outbox (atomic)
   UPDATE seat_inventories
     SET available_seats = available_seats - {seats}
   WHERE id = {inventory.id}

   INSERT INTO outbox_events
     (event_type='SEATS_HELD',
      payload={ inventoryId, seatsHeld: seats, remainingSeats: updated.available_seats,
                heldUntil: NOW() + 15 minutes, bookingId })

   COMMIT (lock released — next waiting transaction can proceed)

3. Return 200 { success: true, remainingSeats: <updated count>, heldUntil: <ISO string> }
```

**Concurrent hold behaviour:**
- Request A arrives: locks row, sees 10 seats, decrements to 8, commits, releases lock
- Request B (2ms later): was waiting at FOR UPDATE, now acquires lock, sees 8 (not 10), proceeds
- Sequential ACID correctness — not a race condition

---

### Feature 9: Release Seats (Internal — Booking Service Only)

**Triggered by two scenarios:**
1. Booking Service saga rollback (payment failed)
2. Seat hold timeout (user abandoned payment — BullMQ fires after 15 min)

**Flow:**
```
1. Booking Service sends:
   PATCH /internal/flights/instances/{instanceId}/release-seats
   Body: { seats: 2, cabinClass: "ECONOMY", fareClass: "Y", bookingId: "f47ac10b-58cc-4372-a567-0e02b2c3d479" }

2. Flight Service executes (inside Prisma $transaction):

   Step A: Idempotency Guard
   SELECT payload FROM outbox_events WHERE event_type = 'SEATS_RELEASED' AND payload->>'bookingId' = {bookingId}
   IF found → return 200 with stored remainingSeats immediately (no updates performed)

   Step B: Find inventory
   Find SeatInventory by (flightInstanceId + cabinClass + fareClass) → 404 if not found
   (No FOR UPDATE lock needed — incrementing seats cannot cause double-booking)

   Step C: Increment seats + write outbox (atomic)
   UPDATE seat_inventories
     SET available_seats = LEAST(available_seats + {seats}, total_seats)
   WHERE id = {inventory.id}

   ← LEAST() guard: available_seats can never exceed total_seats
     even if release is called twice (idempotency guard)

   INSERT INTO outbox_events
     (event_type='SEATS_RELEASED',
      payload={ inventoryId, seatsReleased: seats, remainingSeats: updated.available_seats, bookingId })

   COMMIT

3. Return 200 { success: true, remainingSeats: <updated count> }
```

---

### Feature 10: Airport Management (Admin)

**Who can call:** `GET /api/v1/airports` — No auth required (public — for UI autocomplete); `POST` / `PATCH` — `SUPER_ADMIN`

**Why airports need API endpoints:**
`flight_schedules.origin_code` and `destination_code` are FK-constrained to `airports.code`. Without airports seeded and manageable via API, no schedule can ever be created.

**Operations:**
- `GET /api/v1/airports` — list all airports (used for origin/destination autocomplete in UI)
- `GET /api/v1/airports/:code` — get single airport detail
- `POST /api/v1/airports` — add a new airport (SUPER_ADMIN only)
- `PATCH /api/v1/airports/:code` — update airport details (SUPER_ADMIN only)

---

### Feature 11: Aircraft Management (Admin)

**Who can call:** `FLIGHT_ADMIN+` for reads; `FLIGHT_ADMIN+` for creates

**Why aircraft endpoints are needed:**
`flight_schedules.aircraft_id` is FK-constrained to `aircrafts.id`. Without aircraft records existing, no schedule can be created. Admins also need to browse aircraft when picking which to assign to a route.

**Operations:**
- `GET /api/v1/aircrafts` — list all aircraft
- `GET /api/v1/aircrafts/:id` — get single aircraft detail
- `POST /api/v1/aircrafts` — add a new aircraft type
- `PATCH /api/v1/aircrafts/:id` — update aircraft details (e.g., capacity correction)

---

### Feature 12: Schedule Management (Admin)

**Who can call:** `FLIGHT_ADMIN+`

**Why schedule management endpoints are needed:**
Schedules are created via `POST /api/v1/flights/schedules` but without GET/PATCH/DELETE endpoints, admins cannot list existing schedules, verify one before creating an instance, update aircraft assignment, or discontinue a route.

**Operations (beyond existing POST):**
- `GET /api/v1/flights/schedules` — list schedules (filter by route, airline)
- `GET /api/v1/flights/schedules/:id` — get single schedule
- `PATCH /api/v1/flights/schedules/:id` — update aircraft, amenities, times
- `DELETE /api/v1/flights/schedules/:id` — discontinue route via soft-delete (`isActive = false`) (only if no active `SCHEDULED` or `BOARDING` instances exist)

---

### Feature 13: Outbox Worker (Background)

Runs as a recursive `setTimeout` loop (with `OUTBOX_POLL_INTERVAL_MS` delay) inside the service process to prevent overlapping runs.

**What it does:**
1. In a quick database transaction, select pending events and mark them as processing to prevent overlap:
   `SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 100 FOR UPDATE SKIP LOCKED`
   Immediately execute `UPDATE outbox_events SET status = 'PROCESSING' WHERE id IN (...)` and COMMIT the transaction. This releases the row locks and connection immediately, avoiding keeping database transactions open during network calls.
2. In the application code, iterate through the fetched events:
   - Serialize as standard Kafka envelope (see Section 7)
   - Publish to Kafka topic `flight-inventory-events` with `inventoryId` as the partition key
   - On success: Run a quick transaction to `UPDATE outbox_events SET status='PUBLISHED', published_at=NOW() WHERE id = {id}`
   - On failure: Run a quick transaction to `UPDATE outbox_events SET status='PENDING' WHERE id = {id}` (or increment retry count, or mark as `FAILED` if max retries exceeded)
3. Never deletes outbox rows — keep for audit trail and debugging

---

### Feature 14: Health Check

**`GET /health`** — checks PostgreSQL connection and Kafka producer connection

---

### Feature 15: Metrics

**`GET /metrics`** — Prometheus scrape format

Flight Service-specific metrics:
- `flight_hold_requests_total{result}` — counter: `result=success | insufficient_seats | not_found | flight_not_active`
- `flight_release_requests_total` — counter
- `outbox_pending_count` — gauge: how many outbox events are awaiting publish (alert if this grows > 100)
- `outbox_publish_duration_ms` — histogram: time to publish each batch

---

## 3. Database Design & Prisma Schema

### 3.1 Entity-Relationship Diagram

```
┌─────────────────────────────────┐   ┌──────────────────────────────────┐
│            AIRPORTS             │   │            AIRCRAFTS              │
├─────────────────────────────────┤   ├──────────────────────────────────┤
│ id       UUID        PK         │   │ id             UUID       PK     │
│ code     VARCHAR(3)  UNIQUE     │   │ model          VARCHAR(100)      │
│ name     VARCHAR(150) NOT NULL  │   │ total_capacity INT               │
│ city     VARCHAR(100) NOT NULL  │   └────────────────┬─────────────────┘
│ country  VARCHAR(100) NOT NULL  │                    │ 1 (aircraft_id FK)
│ timezone VARCHAR(100) NOT NULL  │                    │
└───────────────┬─────────────────┘                    │
                │ 1 (origin_airport_id +               │
                │    destination_airport_id FKs)       │ has many schedules
                └──────────────────┬───────────────────┘
                                   ▼ N
┌──────────────────────────────────┴──────────────────────────────────────┐
│                          FLIGHT_SCHEDULES                                │
├─────────────────────────────────────────────────────────────────────────┤
│ id                     UUID        PK                                    │
│ flight_number          VARCHAR(20) NOT NULL                              │
│ airline                VARCHAR(100) NOT NULL                             │
│ origin_airport_id      UUID        FK ─► airports.id                     │
│ destination_airport_id UUID        FK ─► airports.id                     │
│ departure_time         VARCHAR(5)  NOT NULL (HH:MM local time)           │
│ arrival_time           VARCHAR(5)  NOT NULL (HH:MM local time)           │
│ duration_minutes       INT         NOT NULL                              │
│ aircraft_id            UUID        FK ─► aircrafts.id                    │
│ amenities              JSONB       DEFAULT []                            │
│ created_by_id          UUID        NOT NULL (admin user id)              │
│ created_at             TIMESTAMPTZ DEFAULT NOW()                         │
│ updated_at             TIMESTAMPTZ AUTO UPDATE                           │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ 1
                                    │ has many
                                    ▼ N
┌───────────────────────────────────┴─────────────────────────────────────┐
│                          FLIGHT_INSTANCES                                │
├─────────────────────────────────────────────────────────────────────────┤
│ id                   UUID        PK                                      │
│ schedule_id          UUID        FK ─► flight_schedules.id              │
│ departure_date       DATE        NOT NULL                                │
│ arrival_date         DATE        NOT NULL                                │
│ status               ENUM        DEFAULT SCHEDULED                       │
│ actual_departure_time TIMESTAMPTZ NULL                                   │
│ actual_arrival_time   TIMESTAMPTZ NULL                                   │
│ gate                 VARCHAR(10) NULL                                    │
│ cancelled_at         TIMESTAMPTZ NULL  ← set when status=CANCELLED      │
│ created_by_id        UUID        NOT NULL (admin user id)               │
│ created_at           TIMESTAMPTZ DEFAULT NOW()                           │
│ updated_at           TIMESTAMPTZ AUTO UPDATE                             │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ 1
                                    │ has many
                                    ▼ N
┌───────────────────────────────────┴─────────────────────────────────────┐
│                          SEAT_INVENTORIES                                │
├─────────────────────────────────────────────────────────────────────────┤
│ id                  UUID      PK                                         │
│ flight_instance_id  UUID      FK ─► flight_instances.id                 │
│ cabin_class         ENUM      NOT NULL (ECONOMY | BUSINESS | FIRST)     │
│ fare_class          VARCHAR(2) NOT NULL                                  │
│ base_price          INT       NOT NULL (paise)                           │
│ total_seats         INT       NOT NULL                                   │
│ available_seats     INT       NOT NULL                                   │
│ baggage_allowance   JSONB     NOT NULL                                   │
│ refundable          BOOLEAN   DEFAULT false                              │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                           OUTBOX_EVENTS                                  │
├─────────────────────────────────────────────────────────────────────────┤
│ id           UUID        PK                                              │
│ event_type   VARCHAR     NOT NULL                                        │
│ payload      JSONB       NOT NULL  ← flight context lives here           │
│ status       ENUM        NOT NULL (PENDING | PROCESSING | PUBLISHED |    │
│                          FAILED)                                         │
│ retry_count  INT         DEFAULT 0                                       │
│ created_at   TIMESTAMPTZ NOT NULL                                        │
│ published_at TIMESTAMPTZ NULL                                            │
└─────────────────────────────────────────────────────────────────────────┘
Note: OutboxEvent has NO FK to any flight table — events are generic.
      The inventoryId / instanceId referenced in the payload are stored
      as JSONB data, not as enforced foreign keys.
```

**Note on `created_by_id`:**
The Flight Service does NOT call User Service to validate this UUID. It trusts the `X-User-Id` header injected by the Gateway (which has already verified the JWT). The UUID is stored for audit — tracking which admin created which flight instance.

### 3.2 Column-by-Column Justification

#### `airports` table
| Column | Type | Why This Design |
|---|---|---|
| `id` | UUID | Unique surrogate primary key. |
| `code` | VARCHAR(3) | IATA 3-letter airport codes. Unique, uppercase index. |
| `timezone` | VARCHAR(100) | Canonical timezone identifiers (e.g. `Asia/Kolkata`). Essential for multi-leg flight tracking. |

#### `aircrafts` table
| Column | Type | Why This Design |
|---|---|---|
| `id` | UUID | Unique identifier. |
| `total_capacity` | INT | Hard ceiling for physical passenger count validation. |

#### `flight_schedules` table
| Column | Type | Why This Design |
|---|---|---|
| `flight_number` | VARCHAR(20) | Static identifier (e.g., `AI-101`). |
| `departure_time` / `arrival_time` | VARCHAR(5) | Stored as local string (HH:MM). Safe from timezone and JS Date parsing bugs. |
| `duration_minutes` | INT | Statically defined duration. Validated to be positive. |
| `created_by_id` | UUID | Admin user who registered this route. Trusted from `X-User-Id` header (Gateway-injected). Not FK-constrained — same rationale as `FlightInstance.createdById`. |
| `created_at` / `updated_at` | TIMESTAMPTZ | Standard audit timestamps. `created_at` powers the default `sortBy=CREATED_AT` on the list endpoint. |

#### `flight_instances` table
| Column | Type | Why This Design |
|---|---|---|
| `departure_date` | DATE | Date of physical operation. |
| `status` | ENUM | `SCHEDULED \| BOARDING \| DEPARTED \| DELAYED \| CANCELLED`. |
| `cancelled_at` | TIMESTAMPTZ? | Set atomically when `DELETE /instances/:id` is called. Needed for audit and response. |

#### `seat_inventories` table
| Column | Type | Why This Design |
|---|---|---|
| `cabin_class` | ENUM | `ECONOMY \| BUSINESS \| FIRST`. Enforced at DB level. |
| `fare_class` | VARCHAR(2) | Real-world booking codes (Y, M, Q). Allows yield management. |
| `base_price` | INT | Minor units (paise for INR). **Never a float.** `₹4,999 = 499900`. |
| `available_seats` | INT | Changes on every hold/release. Enforced by DB check constraint: `0 ≤ available ≤ total`. |
| `baggage_allowance` | JSONB | `{ "cabin": "7kg", "checked": "15kg" }`. |

#### `outbox_events` table
| Column | Why |
|---|---|
| `event_type` | String (not ENUM) — add new event types without migration |
| `payload` | JSONB — full Kafka envelope stored |
| `status` | ENUM: `PENDING \| PROCESSING \| PUBLISHED \| FAILED` |
| `retry_count` | INT — Track number of publication retries to handle poison pill events |

### 3.3 Database Constraints (CHECK constraints)

These are enforced at DB level. Append to the migration SQL after `npx prisma migrate dev --create-only`:

```sql
-- available_seats must be non-negative and cannot exceed total_seats
ALTER TABLE seat_inventories
  ADD CONSTRAINT chk_available_seats
  CHECK (available_seats >= 0 AND available_seats <= total_seats);

-- base_price must be positive
ALTER TABLE seat_inventories
  ADD CONSTRAINT chk_base_price
  CHECK (base_price > 0);

-- total_seats must be positive
ALTER TABLE seat_inventories
  ADD CONSTRAINT chk_total_seats
  CHECK (total_seats > 0);

-- duration must be positive
ALTER TABLE flight_schedules
  ADD CONSTRAINT chk_duration
  CHECK (duration_minutes > 0);
```

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
  SCHEDULED
  BOARDING
  DEPARTED
  DELAYED
  CANCELLED
}

enum OutboxStatus {
  PENDING
  PROCESSING
  PUBLISHED
  FAILED
}

// ─── Models ───────────────────────────────────────────────────────────────────

model Airport {
  id        String   @id @default(uuid())
  code      String   @unique @db.VarChar(3)
  name      String   @db.VarChar(150)
  city      String   @db.VarChar(100)
  country   String   @db.VarChar(100)
  timezone  String   @db.VarChar(100)

  departingSchedules FlightSchedule[] @relation("OriginAirport")
  arrivingSchedules  FlightSchedule[] @relation("DestinationAirport")

  @@map("airports")
}

model Aircraft {
  id            String   @id @default(uuid())
  model         String   @db.VarChar(100)
  totalCapacity Int      @map("total_capacity")

  schedules     FlightSchedule[]

  @@map("aircrafts")
}

model FlightSchedule {
  id                   String   @id @default(uuid())
  flightNumber         String   @map("flight_number") @db.VarChar(20)
  airline              String   @db.VarChar(100)
  originAirportId      String   @map("origin_airport_id")
  destinationAirportId String   @map("destination_airport_id")
  departureTime        String   @map("departure_time") @db.VarChar(5)
  arrivalTime          String   @map("arrival_time") @db.VarChar(5)
  durationMinutes      Int      @map("duration_minutes")
  aircraftId           String   @map("aircraft_id")
  amenities            Json     @default("[]")
  isActive             Boolean  @default(true) @map("is_active")
  createdById          String   @map("created_by_id")
  createdAt            DateTime @default(now()) @map("created_at")
  updatedAt            DateTime @updatedAt @map("updated_at")

  origin               Airport  @relation("OriginAirport", fields: [originAirportId], references: [id])
  destination          Airport  @relation("DestinationAirport", fields: [destinationAirportId], references: [id])
  aircraft             Aircraft @relation(fields: [aircraftId], references: [id])
  instances            FlightInstance[]

  @@unique([flightNumber, originAirportId, destinationAirportId], name: "uq_schedule_identity")
  @@index([originAirportId, destinationAirportId, isActive], name: "idx_schedule_active_route")
  @@index([createdAt], name: "idx_schedule_created_at")
  @@map("flight_schedules")
}

model FlightInstance {
  id                  String         @id @default(uuid())
  scheduleId          String         @map("schedule_id")
  departureDate       DateTime       @map("departure_date") @db.Date
  arrivalDate         DateTime       @map("arrival_date") @db.Date
  status              FlightStatus   @default(SCHEDULED)
  actualDepartureTime DateTime?      @map("actual_departure_time") @db.Timestamptz
  actualArrivalTime   DateTime?      @map("actual_arrival_time") @db.Timestamptz
  gate                String?        @db.VarChar(10)
  cancelledAt         DateTime?      @map("cancelled_at") @db.Timestamptz
  createdById         String         @map("created_by_id")
  createdAt           DateTime       @default(now()) @map("created_at")
  updatedAt           DateTime       @updatedAt @map("updated_at")

  schedule            FlightSchedule @relation(fields: [scheduleId], references: [id], onDelete: Cascade)
  inventories         SeatInventory[]

  @@unique([scheduleId, departureDate], name: "uq_flight_instance")
  @@index([departureDate])
  @@map("flight_instances")
}

model SeatInventory {
  id               String         @id @default(uuid())
  flightInstanceId String         @map("flight_instance_id")
  cabinClass       CabinClass     @map("cabin_class")
  fareClass        String         @map("fare_class") @db.VarChar(2)
  basePrice        Int            @map("base_price")
  totalSeats       Int            @map("total_seats")
  availableSeats   Int            @map("available_seats")
  baggageAllowance Json           @map("baggage_allowance")
  refundable       Boolean        @default(false)

  flightInstance   FlightInstance @relation(fields: [flightInstanceId], references: [id], onDelete: Cascade)

  @@unique([flightInstanceId, cabinClass, fareClass], name: "uq_inventory_bucket")
  @@map("seat_inventories")
}

model OutboxEvent {
  id          String       @id @default(uuid())
  eventType   String       @map("event_type")
  payload     Json
  status      OutboxStatus @default(PENDING)
  retryCount  Int          @default(0) @map("retry_count")
  createdAt   DateTime     @default(now()) @map("created_at")
  publishedAt DateTime?    @map("published_at")

  @@index([status, createdAt])
  @@map("outbox_events")
}
```

### 3.5 Index Summary

| Index | Columns | Type | Purpose |
|---|---|---|---|
| `uq_schedule_identity` | `(flightNumber, originAirportId, destinationAirportId)` | Unique | Prevent duplicate route declarations |
| `idx_schedule_active_route` | `(originAirportId, destinationAirportId, isActive)` | B-Tree | High-speed active route lookup |
| `uq_flight_instance` | `(scheduleId, departureDate)` | Unique | Block duplicate flights on same route same day |
| `idx_instance_date` | `departureDate` | B-Tree | Quick filtering of operational dates |
| `uq_inventory_bucket` | `(flightInstanceId, cabinClass, fareClass)` | Unique | Enforce unique pricing buckets per class |
| `idx_schedule_created_at` | `createdAt` on `flight_schedules` | B-Tree | Powers `sortBy=CREATED_AT` on admin schedule list |
| `(status, createdAt)` on outbox | Compound | B-Tree | Outbox Worker polling query |

*Note on Outbox Worker Polling Optimization*: Although the Prisma schema defines a compound index `(status, createdAt)` for database engine compatibility, in a PostgreSQL production environment, it is highly recommended to replace it with a **partial index** in the SQL migration file:
```sql
CREATE INDEX idx_pending_outbox ON outbox_events (created_at) WHERE status = 'PENDING';
```
This keeps the index size minimal by only indexing active, unprocessed outbox tasks, reducing memory and modification overhead.

### 3.6 Schema Evolution & Migration Strategy

For production zero-downtime database deployment, Flight Service migration plans conform to the following patterns:

- **Deployment Order (Expand-Contract Pattern):**
  - **Adding Columns/Fields (e.g. `is_active` or `retryCount`):** 
    1. Apply the database migration to add the column (ensuring it is either nullable or has a safe default value).
    2. Deploy the updated microservice code that writes to and reads this new column.
  - **Dropping/Renaming Columns:**
    1. Deploy the new code that stops querying and writing to the old column.
    2. Apply the database migration to drop the column from PostgreSQL.
- **Batched Backfills for Schema Upgrades:**
  - When altering types or introducing large-scale column default transformations on tables like `seat_inventories` (which grows rapidly with daily instances), backfills must be run in cursor-based batches of $\le 5,000$ rows (e.g. using `available_seats` update batches) rather than single large updates. This prevents locking the entire table and causing transaction timeouts for concurrent seat holdings.

---

## 4. Security & RBAC Architecture

### 4.1 Role Enforcement on Admin Routes

The Flight Service trusts `X-User-Role` and `X-User-Id` headers injected by the API Gateway. It does NOT re-verify the JWT.

**`requireRole` middleware:**
```
Read X-User-Role header
  └── If header missing → 401 Unauthorized ("Authentication required")
  └── If role not in allowed list → 403 Forbidden ("Insufficient permissions")
  └── If role matches → attach to req.userRole + req.userId, call next()
```

**Role requirements per endpoint (updated to match actual API paths):**

| Endpoint | Minimum Role |
|---|---|
| `GET /api/v1/airports` | No auth required (public — for UI autocomplete) |
| `GET /api/v1/airports/:code` | No auth required (public — for UI autocomplete) |
| `POST /api/v1/airports` | `SUPER_ADMIN` |
| `PATCH /api/v1/airports/:code` | `SUPER_ADMIN` |
| `GET /api/v1/aircrafts` | `FLIGHT_ADMIN` |
| `GET /api/v1/aircrafts/:id` | `FLIGHT_ADMIN` |
| `POST /api/v1/aircrafts` | `FLIGHT_ADMIN` |
| `PATCH /api/v1/aircrafts/:id` | `FLIGHT_ADMIN` |
| `POST /api/v1/flights/schedules` | `FLIGHT_ADMIN` |
| `GET /api/v1/flights/schedules` | `FLIGHT_ADMIN` |
| `GET /api/v1/flights/schedules/:id` | `FLIGHT_ADMIN` |
| `PATCH /api/v1/flights/schedules/:id` | `FLIGHT_ADMIN` |
| `DELETE /api/v1/flights/schedules/:id` | `SUPER_ADMIN` |
| `POST /api/v1/flights/instances` | `FLIGHT_ADMIN` |
| `GET /api/v1/flights/instances` | `FLIGHT_ADMIN` |
| `GET /api/v1/flights/instances/:id` | No auth required (public) |
| `PATCH /api/v1/flights/instances/:id` | `FLIGHT_ADMIN` |
| `DELETE /api/v1/flights/instances/:id` | `SUPER_ADMIN` |
| `PATCH /api/v1/flights/instances/:instanceId/inventories/:inventoryId` | `FLIGHT_ADMIN` |
| `/internal/flights/instances/:id/hold-seats` | No JWT — `X-Internal-Secret` header only |
| `/internal/flights/instances/:id/release-seats` | No JWT — `X-Internal-Secret` header only |

### 4.2 Internal Endpoint Protection

The `/internal/*` routes are **never proxied by the API Gateway**. The Gateway only maps `/api/v1/flights/*` and `/api/v1/airports/*` and `/api/v1/aircrafts/*`.

**In local development:** internal routes are protected by a shared secret header (`X-Internal-Secret`):

```typescript
// middlewares/internalAuth.ts
export function internalAuth(req: Request, res: Response, next: NextFunction) {
  const secret = req.headers['x-internal-secret'];
  if (!secret || secret !== env.INTERNAL_SECRET) {
    return res.status(401).json({ success: false, error: 'Internal access only' });
  }
  next();
}
```

**In Kubernetes production:** NetworkPolicy restricts which pods can reach port 3002/internal. mTLS adds certificate-based authentication. The shared secret remains as defense-in-depth.

**In the Express app:**
```
app.use('/api/v1/airports',  airportRouter)               ← Gateway proxies these
app.use('/api/v1/aircrafts', aircraftRouter)              ← Gateway proxies these
app.use('/api/v1/flights',   publicFlightRouter)          ← Gateway proxies these
app.use('/internal/flights', internalAuth, internalRouter) ← internal services only
app.use('/',                 healthRouter)
```

### 4.3 Input Validation Security

- All admin input validated with Zod before any DB operation
- IATA codes normalized to uppercase (prevent case-sensitivity bugs)
- `base_price` validated as positive integer (prevents negative pricing)
- `amenities` array maximum length: 20 items, each max 50 chars
- `baggage_allowance` keys whitelisted (`cabin`, `checked`, `excess_per_kg`)
- Prisma parameterized queries prevent SQL injection; `$queryRaw` uses tagged template literals (also parameterized)

---

## 5. Complete REST API Specification

All public endpoints are prefixed `/api/v1` at the Gateway level.

### Standard Response Envelopes

**Public endpoints** use the cluster-wide standard:
```json
{ "success": true, "message": "...", "data": {}, "meta": {}, "traceId": "..." }
{ "success": false, "error": { "statusCode": 400, "name": "VALIDATION_ERROR", "message": "...", "details": [] }, "traceId": "..." }
```

**Internal endpoints** (`/internal/*`) use a minimal envelope — no `data` wrapper, no `meta`, no `message` field — to reduce serialization overhead on the hot path:
```json
{ "success": true, "remainingSeats": 140, "heldUntil": "..." }
{ "success": false, "error": { "statusCode": 409, "name": "INSUFFICIENT_SEATS", "message": "...", "details": [...] }, "traceId": "..." }
```

---

## 5.1 Airport Endpoints

### Endpoint 1: GET /api/v1/airports

**Auth required:** No (public — for UI autocomplete)

**Query Parameters:**
- `country` (optional) — filter by country name
- `search` (optional) — substring match on `name`, `city`, or `code`

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Airports retrieved successfully.",
  "data": [
    {
      "id": "airport-del-uuid-111",
      "code": "DEL",
      "name": "Indira Gandhi International",
      "city": "Delhi",
      "country": "India",
      "timezone": "Asia/Kolkata"
    },
    {
      "id": "airport-bom-uuid-222",
      "code": "BOM",
      "name": "Chhatrapati Shivaji Maharaj International",
      "city": "Mumbai",
      "country": "India",
      "timezone": "Asia/Kolkata"
    }
  ],
  "traceId": "tr-f47ac10b"
}
```

---

### Endpoint 2: GET /api/v1/airports/:id

**Auth required:** No (public — used for UI autocomplete)

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Airport retrieved successfully.",
  "data": {
    "id": "airport-del-uuid-111",
    "code": "DEL",
    "name": "Indira Gandhi International",
    "city": "Delhi",
    "country": "India",
    "timezone": "Asia/Kolkata"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
404 NOT_FOUND   → Airport ID does not exist
```

---

### Endpoint 3: POST /api/v1/airports

**Auth required:** Yes — `SUPER_ADMIN` only

**Request Body:**
```json
{
  "code":     "HYD",
  "name":     "Rajiv Gandhi International",
  "city":     "Hyderabad",
  "country":  "India",
  "timezone": "Asia/Kolkata"
}
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Airport created successfully.",
  "data": {
    "id": "airport-hyd-uuid-333",
    "code": "HYD",
    "name": "Rajiv Gandhi International",
    "city": "Hyderabad",
    "country": "India",
    "timezone": "Asia/Kolkata"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR   → Zod failed (code not 3 chars, missing fields)
409 CONFLICT           → Airport code already exists
```

---

### Endpoint 4: PATCH /api/v1/airports/:id

**Auth required:** Yes — `SUPER_ADMIN` only

**Request Body (all fields optional):**
```json
{
  "name":     "Rajiv Gandhi International Airport",
  "timezone": "Asia/Kolkata"
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Airport updated successfully.",
  "data": {
    "id": "airport-hyd-uuid-333",
    "code": "HYD",
    "name": "Rajiv Gandhi International Airport",
    "city": "Hyderabad",
    "country": "India",
    "timezone": "Asia/Kolkata"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR   → Zod failed
404 NOT_FOUND          → Airport ID does not exist
```

---

## 5.2 Aircraft Endpoints

### Endpoint 5: GET /api/v1/aircrafts

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Aircraft retrieved successfully.",
  "data": [
    {
      "id": "a59e1981-d1c9-467b-8912-32a220b3309a",
      "model": "Airbus A320",
      "totalCapacity": 180
    },
    {
      "id": "b72f2982-e2d0-578c-9023-43b331c4410b",
      "model": "Boeing 737-800",
      "totalCapacity": 162
    }
  ],
  "traceId": "tr-f47ac10b"
}
```

---

### Endpoint 6: GET /api/v1/aircrafts/:id

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Aircraft retrieved successfully.",
  "data": {
    "id": "a59e1981-d1c9-467b-8912-32a220b3309a",
    "model": "Airbus A320",
    "totalCapacity": 180
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
404 NOT_FOUND   → Aircraft ID does not exist
```

---

### Endpoint 7: POST /api/v1/aircrafts

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body:**
```json
{
  "model":         "Airbus A320",
  "totalCapacity": 180
}
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Aircraft created successfully.",
  "data": {
    "id": "a59e1981-d1c9-467b-8912-32a220b3309a",
    "model": "Airbus A320",
    "totalCapacity": 180
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR          → Zod failed
422 BUSINESS_RULE_VIOLATION   → totalCapacity < 1
```

---

### Endpoint 8: PATCH /api/v1/aircrafts/:id

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body (all fields optional):**
```json
{
  "model":         "Airbus A320neo",
  "totalCapacity": 194
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Aircraft updated successfully.",
  "data": {
    "id": "a59e1981-d1c9-467b-8912-32a220b3309a",
    "model": "Airbus A320neo",
    "totalCapacity": 194
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR   → Zod failed
404 NOT_FOUND          → Aircraft ID does not exist
```

---

## 5.3 Flight Schedule Endpoints

### Endpoint 9: POST /api/v1/flights/schedules

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body:**
```json
{
  "flightNumber":    "6E-204",
  "airline":         "IndiGo",
  "originCode":      "DEL",
  "destinationCode": "BOM",
  "departureTime":   "06:30",
  "arrivalTime":     "09:15",
  "durationMinutes": 165,
  "aircraftId":      "a59e1981-d1c9-467b-8912-32a220b3309a",
  "amenities":       ["usb", "snack"]
}
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Flight schedule created successfully.",
  "data": {
    "scheduleId":      "abc123-def456-schedule-uuid",
    "flightNumber":    "6E-204",
    "airline":         "IndiGo",
    "originCode":      "DEL",
    "destinationCode": "BOM",
    "departureTime":   "06:30",
    "arrivalTime":     "09:15",
    "durationMinutes": 165,
    "aircraftId":      "a59e1981-d1c9-467b-8912-32a220b3309a",
    "amenities":       ["usb", "snack"],
    "createdById":     "7b58c281-a5bf-4050-a922-a72a1cd40a92",
    "createdAt":       "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR          → Zod validation failed
404 NOT_FOUND                 → originCode, destinationCode, or aircraftId not found
409 CONFLICT                  → Same (flightNumber, originCode, destinationCode) already exists
422 BUSINESS_RULE_VIOLATION   → originCode === destinationCode
```

---

### Endpoint 10: GET /api/v1/flights/schedules

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Query Parameters:**
- `originCode` (optional)
- `destinationCode` (optional)
- `airline` (optional, substring match)
- `page` / `limit` (default: page=1, limit=20, max=100)
- `sortBy` — `FLIGHT_NUMBER | AIRLINE | CREATED_AT` (default: `CREATED_AT`)
- `sortOrder` — `ASC | DESC` (default: `DESC`)

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight schedules retrieved successfully.",
  "data": [
    {
      "scheduleId":      "abc123-def456-schedule-uuid",
      "flightNumber":    "6E-204",
      "airline":         "IndiGo",
      "originCode":      "DEL",
      "destinationCode": "BOM",
      "departureTime":   "06:30",
      "arrivalTime":     "09:15",
      "durationMinutes": 165,
      "aircraftId":      "a59e1981-d1c9-467b-8912-32a220b3309a",
      "amenities":       ["usb", "snack"]
    }
  ],
  "meta": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "totalPages": 1
  },
  "traceId": "tr-f47ac10b"
}
```

---

### Endpoint 11: GET /api/v1/flights/schedules/:id

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight schedule retrieved successfully.",
  "data": {
    "scheduleId":      "abc123-def456-schedule-uuid",
    "flightNumber":    "6E-204",
    "airline":         "IndiGo",
    "originCode":      "DEL",
    "destinationCode": "BOM",
    "departureTime":   "06:30",
    "arrivalTime":     "09:15",
    "durationMinutes": 165,
    "aircraft": {
      "id":            "a59e1981-d1c9-467b-8912-32a220b3309a",
      "model":         "Airbus A320",
      "totalCapacity": 180
    },
    "amenities":  ["usb", "snack"],
    "createdAt":  "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
404 NOT_FOUND   → Schedule ID does not exist
```

---

### Endpoint 12: PATCH /api/v1/flights/schedules/:id

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**What is updatable:**
- `departureTime`, `arrivalTime`, `durationMinutes` — schedule time adjustment
- `aircraftId` — aircraft swap
- `amenities` — service change

**What is NOT updatable:**
- `flightNumber`, `originCode`, `destinationCode`, `airline` — define the route identity; cancel and recreate if these need to change

**Request Body (all fields optional):**
```json
{
  "departureTime":   "07:00",
  "arrivalTime":     "09:45",
  "durationMinutes": 165,
  "aircraftId":      "b72f2982-e2d0-578c-9023-43b331c4410b",
  "amenities":       ["usb", "snack", "meal"]
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight schedule updated successfully.",
  "data": {
    "scheduleId":      "abc123-def456-schedule-uuid",
    "flightNumber":    "6E-204",
    "airline":         "IndiGo",
    "originCode":      "DEL",
    "destinationCode": "BOM",
    "departureTime":   "07:00",
    "arrivalTime":     "09:45",
    "durationMinutes": 165,
    "aircraftId":      "b72f2982-e2d0-578c-9023-43b331c4410b",
    "amenities":       ["usb", "snack", "meal"]
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR   → Zod failed
404 NOT_FOUND          → Schedule ID does not exist or new aircraftId not found
```

---

### Endpoint 13: DELETE /api/v1/flights/schedules/:id

**Auth required:** Yes — `SUPER_ADMIN` only

**Rules:**
- Cannot deactivate a schedule that has any active `SCHEDULED` or `BOARDING` flight instances — return 422
- Can only deactivate if all instances are `CANCELLED`, `DEPARTED`, or there are no instances. Performs soft-delete by setting `isActive = false` in `flight_schedules`.

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight schedule deactivated successfully.",
  "data": {
    "scheduleId":  "abc123-def456-schedule-uuid",
    "isActive":    false,
    "deactivatedAt": "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
403 FORBIDDEN                → role is FLIGHT_ADMIN (not SUPER_ADMIN)
404 NOT_FOUND                → Schedule ID does not exist
422 BUSINESS_RULE_VIOLATION  → Schedule has active (SCHEDULED/BOARDING) instances
```

---

## 5.4 Flight Instance Endpoints

### Endpoint 14: POST /api/v1/flights/instances

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body:**
```json
{
  "scheduleId": "abc123-def456-schedule-uuid",
  "departureDate": "2026-10-12",
  "arrivalDate": "2026-10-12",
  "inventories": [
    {
      "cabinClass": "ECONOMY",
      "fareClass": "Y",
      "basePrice": 499900,
      "totalSeats": 180,
      "baggageAllowance": {
        "cabin": "7kg",
        "checked": "15kg"
      },
      "refundable": false
    }
  ]
}
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Flight instance operationalized successfully.",
  "data": {
    "instanceId": "instance-uuid-12345",
    "scheduleId": "abc123-def456-schedule-uuid",
    "departureDate": "2026-10-12",
    "arrivalDate": "2026-10-12",
    "status": "SCHEDULED",
    "gate": null,
    "actualDepartureTime": null,
    "actualArrivalTime": null,
    "cancelledAt": null,
    "inventories": [
      {
        "inventoryId": "inventory-uuid-999",
        "cabinClass": "ECONOMY",
        "fareClass": "Y",
        "basePrice": 499900,
        "totalSeats": 180,
        "availableSeats": 180,
        "baggageAllowance": { "cabin": "7kg", "checked": "15kg" },
        "refundable": false
      }
    ],
    "createdById": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
    "createdAt": "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR          → Zod validation failed (date format, missing inventory)
404 NOT_FOUND                 → scheduleId not found
409 CONFLICT                  → Instance already scheduled for this route on this date
422 BUSINESS_RULE_VIOLATION   → departureDate is in the past, or arrivalDate < departureDate
```

---

### Endpoint 15: GET /api/v1/flights/instances/:id

**Auth required:** No (public)

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight retrieved successfully.",
  "data": {
    "instanceId": "instance-uuid-12345",
    "departureDate": "2026-10-12",
    "arrivalDate": "2026-10-12",
    "status": "SCHEDULED",
    "gate": "Gate 12",
    "actualDepartureTime": null,
    "actualArrivalTime": null,
    "schedule": {
      "scheduleId": "abc123-def456-schedule-uuid",
      "flightNumber": "6E-204",
      "airline": "IndiGo",
      "originCode": "DEL",
      "destinationCode": "BOM",
      "departureTime": "06:30",
      "arrivalTime": "09:15",
      "durationMinutes": 165,
      "amenities": ["usb", "snack"]
    },
    "inventories": [
      {
        "inventoryId": "inventory-uuid-999",
        "cabinClass": "ECONOMY",
        "fareClass": "Y",
        "basePrice": 499900,
        "totalSeats": 180,
        "availableSeats": 142,
        "baggageAllowance": { "cabin": "7kg", "checked": "15kg" },
        "refundable": false
      }
    ]
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
404 NOT_FOUND   → Flight instance not found, or status is CANCELLED (hidden from public)
```

---

### Endpoint 16: GET /api/v1/flights/instances

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Query Parameters:**
- `originCode` (optional, IATA 3-letter code)
- `destinationCode` (optional, IATA 3-letter code)
- `date` (optional, YYYY-MM-DD)
- `cabinClass` (optional, `ECONOMY | BUSINESS | FIRST`)
- `airline` (optional, substring match)
- `status` (optional, `SCHEDULED | BOARDING | DEPARTED | DELAYED | CANCELLED`)
- `page` (default: 1, min: 1)
- `limit` (default: 20, min: 1, max: 100)
- `sortBy` — `DEPARTURE_DATE | CREATED_AT` (default: `CREATED_AT`)
- `sortOrder` — `ASC | DESC` (default: `DESC`)

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight instances retrieved successfully.",
  "data": [
    {
      "instanceId": "instance-uuid-12345",
      "departureDate": "2026-10-12",
      "arrivalDate": "2026-10-12",
      "status": "SCHEDULED",
      "flightNumber": "6E-204",
      "airline": "IndiGo",
      "originCode": "DEL",
      "destinationCode": "BOM",
      "departureTime": "06:30",
      "arrivalTime": "09:15",
      "durationMinutes": 165,
      "inventories": [
        {
          "inventoryId": "inventory-uuid-999",
          "cabinClass": "ECONOMY",
          "fareClass": "Y",
          "basePrice": 499900,
          "availableSeats": 142
        }
      ]
    }
  ],
  "meta": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "totalPages": 1
  },
  "traceId": "tr-f47ac10b"
}
```

---

### Endpoint 17: PATCH /api/v1/flights/instances/:id

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Request Body (all fields optional — operational state only):**
```json
{
  "status": "DELAYED",
  "gate": "Gate 15A",
  "actualDepartureTime": "2026-10-12T07:00:00.000Z",
  "actualArrivalTime":   "2026-10-12T09:50:00.000Z"
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight instance updated successfully.",
  "data": {
    "instanceId":           "instance-uuid-12345",
    "scheduleId":           "abc123-def456-schedule-uuid",
    "departureDate":        "2026-10-12",
    "arrivalDate":          "2026-10-12",
    "status":               "DELAYED",
    "gate":                 "Gate 15A",
    "actualDepartureTime":  "2026-10-12T07:00:00.000Z",
    "actualArrivalTime":    "2026-10-12T09:50:00.000Z",
    "cancelledAt":          null,
    "createdById":          "7b58c281-a5bf-4050-a922-a72a1cd40a92",
    "createdAt":            "2026-05-28T10:00:00.000Z",
    "updatedAt":            "2026-10-12T06:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR   → Invalid field values, unrecognized fields, or status=CANCELLED
                         (CANCELLED is excluded from the enum — Zod rejects it before
                          the request reaches the service; use DELETE endpoint to cancel)
403 FORBIDDEN          → Insufficient role
404 NOT_FOUND          → Flight instance does not exist
```

---

### Endpoint 18: DELETE /api/v1/flights/instances/:id

**Auth required:** Yes — `SUPER_ADMIN` only

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight instance cancelled successfully. Seat inventories closed.",
  "data": {
    "instanceId":  "instance-uuid-12345",
    "status":      "CANCELLED",
    "cancelledAt": "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
403 FORBIDDEN              → role is FLIGHT_ADMIN (not SUPER_ADMIN)
404 NOT_FOUND              → flight instance not found
400 ALREADY_CANCELLED      → flight is already CANCELLED
```

---

## 5.5 Seat Inventory Endpoint

### Endpoint 19: PATCH /api/v1/flights/instances/:instanceId/inventories/:inventoryId

**Auth required:** Yes — `FLIGHT_ADMIN` or `SUPER_ADMIN`

**Purpose:** Update pricing, seat counts, baggage policy, or refund policy for a specific cabin class on a specific flight instance. Publishes a `FLIGHT_UPDATED` Kafka event when price or seat counts change so the Search Service reflects the update.

**Request Body (all fields optional):**
```json
{
  "basePrice":        599900,
  "totalSeats":       200,
  "availableSeats":   195,
  "baggageAllowance": { "cabin": "7kg", "checked": "20kg" },
  "refundable":       true
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Seat inventory updated successfully.",
  "data": {
    "inventoryId":       "inventory-uuid-999",
    "flightInstanceId":  "instance-uuid-12345",
    "cabinClass":        "ECONOMY",
    "fareClass":         "Y",
    "basePrice":         599900,
    "totalSeats":        200,
    "availableSeats":    195,
    "baggageAllowance":  { "cabin": "7kg", "checked": "20kg" },
    "refundable":        true
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR          → Zod failed (negative price, invalid seats, unknown baggage keys)
403 FORBIDDEN                 → inventoryId does not belong to the given instanceId
404 NOT_FOUND                 → instanceId or inventoryId does not exist
422 BUSINESS_RULE_VIOLATION   → availableSeats > totalSeats, or availableSeats < 0
```

---

## 5.6 Internal Endpoints

### Endpoint 20: PATCH /internal/flights/instances/:id/hold-seats

**Auth required:** No JWT — `X-Internal-Secret` header only (internal network)

**Request Body:**
```json
{
  "seats":      2,
  "cabinClass": "ECONOMY",
  "fareClass":  "Y",
  "bookingId":  "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

**Success Response — 200 OK** (internal envelope — no `data` wrapper):
```json
{
  "success":        true,
  "remainingSeats": 140,
  "heldUntil":      "2026-05-28T10:15:00.000Z"
}
```

**Error Responses** (internal envelope):
```
400 VALIDATION_ERROR     → seats invalid or required fields missing
400 FLIGHT_NOT_ACTIVE    → Flight instance status is not SCHEDULED
404 NOT_FOUND            → Flight instance or matching seat inventory bucket not found
409 INSUFFICIENT_SEATS   → Not enough seats available:
  {
    "success": false,
    "error": {
      "statusCode": 409,
      "name": "INSUFFICIENT_SEATS",
      "message": "Not enough seats available",
      "details": [{ "availableSeats": 1, "requestedSeats": 2 }]
    },
    "traceId": "tr-f47ac10b"
  }
```

---

### Endpoint 21: PATCH /internal/flights/instances/:id/release-seats

**Auth required:** No JWT — `X-Internal-Secret` header only (internal network)

**Request Body:**
```json
{
  "seats":      2,
  "cabinClass": "ECONOMY",
  "fareClass":  "Y",
  "bookingId":  "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

**Success Response — 200 OK** (internal envelope):
```json
{
  "success":        true,
  "remainingSeats": 142
}
```

**This endpoint is idempotent.** `LEAST(available + seats, totalSeats)` ensures capacity cannot overflow even if called twice.

**Error Responses:**
```
400 VALIDATION_ERROR   → required fields missing
404 NOT_FOUND          → Flight instance or inventory bucket not found
```

---

## 5.7 Observability Endpoints

### Endpoint 22: GET /health

**Auth required:** No

**Healthy Response — 200 OK:**
```json
{
  "status": "healthy",
  "service": "flight-service",
  "version": "1.0.0",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database": "ok",
    "kafka": "ok"
  }
}
```

**Degraded Response — 503 Service Unavailable:**
```json
{
  "status": "degraded",
  "service": "flight-service",
  "version": "1.0.0",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database": "ok",
    "kafka": "error"
  }
}
```

---

### Endpoint 23: GET /metrics

**Auth required:** No

Returns Prometheus scrape text with:
- Standard `http_request_duration_ms` histogram (method, route, status_code)
- `flight_hold_requests_total{result}` — `result=success | insufficient_seats | not_found | flight_not_active`
- `flight_release_requests_total`
- `outbox_pending_count` — gauge (alert if > 100)
- `outbox_publish_duration_ms` — histogram

---

## 6. Zod Validation Schemas

**File: `src/routes/schemas/flight.schemas.ts`**

### AirportParamsSchema
```
code: string, length exactly 3, toUpperCase, regex /^[A-Z]{3}$/
```

### CreateAirportSchema
| Field | Rule |
|---|---|
| `code` | string, length 3, toUpperCase, regex `/^[A-Z]{3}$/` |
| `name` | string, min 2, max 150, trim |
| `city` | string, min 2, max 100, trim |
| `country` | string, min 2, max 100, trim |
| `timezone` | string, min 5, max 100 (e.g. `Asia/Kolkata`) |

### UpdateAirportSchema
All fields optional (same rules as `CreateAirportSchema` minus `code`).

### CreateAircraftSchema
| Field | Rule |
|---|---|
| `model` | string, min 2, max 100, trim |
| `totalCapacity` | integer, min 1 |

### UpdateAircraftSchema
All fields optional (same rules as `CreateAircraftSchema`).

### CreateScheduleSchema
| Field | Rule |
|---|---|
| `flightNumber` | string, min 3, max 20, regex `/^[A-Z0-9]{2}-\d{1,4}[A-Z]?$/i`, transform toUpperCase |
| `airline` | string, min 2, max 100, trim |
| `originCode` | string, length 3, toUpperCase, regex `/^[A-Z]{3}$/` |
| `destinationCode` | string, length 3, toUpperCase, regex `/^[A-Z]{3}$/` |
| `departureTime` | string, regex `/^(?:[01]\d|2[0-3]):[0-5]\d$/` (HH:MM) |
| `arrivalTime` | string, regex `/^(?:[01]\d|2[0-3]):[0-5]\d$/` (HH:MM) |
| `durationMinutes` | integer, min 1 |
| `aircraftId` | string, UUID |
| `amenities` | optional, array of strings, max 20 items, each max 50 chars |

*Cross-field*: `originCode !== destinationCode`

### UpdateScheduleSchema
All fields optional:
- `departureTime`, `arrivalTime`, `durationMinutes`, `aircraftId`, `amenities` (same rules as CreateScheduleSchema)

Fields NOT allowed: `flightNumber`, `originCode`, `destinationCode`, `airline` — reject if present with 400.

### ListSchedulesQuerySchema
| Field | Rule |
|---|---|
| `originCode` | optional, string, length 3, toUpperCase |
| `destinationCode` | optional, string, length 3, toUpperCase |
| `airline` | optional, string, max 100 |
| `page` | optional, string → integer, min 1, default 1 |
| `limit` | optional, string → integer, min 1, max 100, default 20 |
| `sortBy` | optional, enum: `FLIGHT_NUMBER \| AIRLINE \| CREATED_AT`, default `CREATED_AT` |
| `sortOrder` | optional, enum: `ASC \| DESC`, default `DESC` |

### CreateInstanceSchema
| Field | Rule |
|---|---|
| `scheduleId` | string, UUID |
| `departureDate` | string, YYYY-MM-DD, must be today or in the future |
| `arrivalDate` | string, YYYY-MM-DD |
| `inventories` | array, **min length 1**, each item: `cabinClass` (enum), `fareClass` (string max 2), `basePrice` (int min 1), `totalSeats` (int min 1), `baggageAllowance` (object, whitelisted keys: `cabin`, `checked`, `excess_per_kg`), `refundable` (boolean) |

*Cross-field via `.refine()`*: `arrivalDate >= departureDate`

*Cross-field via `.refine()`*: no duplicate `(cabinClass, fareClass)` pairs within the same `inventories` array

*Cross-field/database verification*: sum of `totalSeats` across all inventories must be less than or equal to the associated aircraft's `totalCapacity`.

### UpdateInstanceSchema
All fields optional:
| Field | Rule |
|---|---|
| `status` | optional, enum: `SCHEDULED \| BOARDING \| DEPARTED \| DELAYED` only — `CANCELLED` is explicitly excluded |
| `gate` | optional, string, trim, max 10 |
| `actualDepartureTime` | optional, string, ISO 8601 datetime |
| `actualArrivalTime` | optional, string, ISO 8601 datetime |

### UpdateInventorySchema
All fields optional:
| Field | Rule |
|---|---|
| `basePrice` | optional, integer, min 1 |
| `totalSeats` | optional, integer, min 1 |
| `availableSeats` | optional, integer, min 0 |
| `baggageAllowance` | optional, object with whitelisted keys only: `cabin`, `checked`, `excess_per_kg` |
| `refundable` | optional, boolean |

*Cross-field via `.refine()`*: if both `totalSeats` and `availableSeats` are provided, `availableSeats <= totalSeats`

*Cross-field/database verification*: sum of all `totalSeats` across all inventories on the flight instance must not exceed the associated aircraft's `totalCapacity`.

### HoldSeatsSchema / ReleaseSeatsSchema
| Field | Rule |
|---|---|
| `seats` | integer, min 1, max 9 |
| `cabinClass` | enum: `ECONOMY \| BUSINESS \| FIRST` |
| `fareClass` | string, length 1-2, toUpperCase |
| `bookingId` | string, UUID |

### ListInstancesQuerySchema
| Field | Rule |
|---|---|
| `originCode` | optional, string, length 3, toUpperCase |
| `destinationCode` | optional, string, length 3, toUpperCase |
| `date` | optional, string, YYYY-MM-DD |
| `cabinClass` | optional, enum: `ECONOMY \| BUSINESS \| FIRST` |
| `airline` | optional, string, max 100 |
| `status` | optional, enum: `SCHEDULED \| BOARDING \| DEPARTED \| DELAYED \| CANCELLED` |
| `page` | optional, string → integer, min 1, default 1 |
| `limit` | optional, string → integer, min 1, **max 100**, default 20 |
| `sortBy` | optional, enum: `DEPARTURE_DATE \| CREATED_AT`, default `CREATED_AT` |
| `sortOrder` | optional, enum: `ASC \| DESC`, default `DESC` |

---

## 7. Kafka Event Publishing (Outbox Pattern)

### 7.1 Kafka Topic

**Topic:** `flight-inventory-events`

**Producer:** Flight Service (the only producer for this topic)

**Consumers:** Search Service (upserts MongoDB + invalidates Redis cache), Booking Service (Phase 4 — consumes `FLIGHT_CANCELLED` for refund flows)

### 7.2 Standard Message Envelope

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

**`FLIGHT_UPDATED`** — published on instance create, inventory update, and schedule update:
```json
{
  "inventoryId":      "inventory-uuid-999",
  "airline":          "IndiGo",
  "flightNumber":     "6E-204",
  "origin":           "DEL",
  "destination":      "BOM",
  "departureDate":    "2026-10-12",
  "departureTime":    "06:30",
  "arrivalDate":      "2026-10-12",
  "arrivalTime":      "09:15",
  "durationMinutes":  165,
  "cabinClass":       "ECONOMY",
  "basePrice":        499900,
  "availableSeats":   180,
  "totalSeats":       180,
  "aircraft":         "Airbus A320",
  "stops":            0,
  "amenities":        ["usb", "snack"],
  "baggageAllowance": { "cabin": "7kg", "checked": "15kg" },
  "refundable":       false,
  "status":           "SCHEDULED"
}
```

> **Normalized-to-Flat Mapping:** The Outbox Worker joins `SeatInventory`, `FlightInstance`, `FlightSchedule`, `Airport`, and `Aircraft` to produce this denormalized flat payload. `inventoryId` is `SeatInventory.id` — the Search Service uses it as the MongoDB document `_id`.

**`SEATS_HELD`** — published on successful hold:
```json
{
  "inventoryId":    "inventory-uuid-999",
  "seatsHeld":      2,
  "remainingSeats": 140,
  "heldUntil":      "2026-05-28T10:15:00.000Z",
  "bookingId":      "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

**`SEATS_RELEASED`** — published on successful release:
```json
{
  "inventoryId":    "inventory-uuid-999",
  "seatsReleased":  2,
  "remainingSeats": 142,
  "bookingId":      "f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

**`FLIGHT_CANCELLED`** — published once **per `SeatInventory` bucket** when a flight instance is cancelled:
```json
{
  "inventoryId":   "inventory-uuid-999",
  "instanceId":    "instance-uuid-12345",
  "origin":        "DEL",
  "destination":   "BOM",
  "departureDate": "2026-10-12"
}
```

> **Why one event per inventory bucket?** A `FlightInstance` has multiple `SeatInventory` rows (ECONOMY/BUSINESS/FIRST). Each is a separate MongoDB document in the Search Service, keyed by `inventoryId`. One event per bucket lets the Search Service delete by `inventoryId` without complex logic.

### 7.4 Outbox Worker Behaviour

Runs every `OUTBOX_POLL_INTERVAL_MS` (default 5s):

1. Queries pending events inside a short transaction using `FOR UPDATE SKIP LOCKED`, updates their status to `PROCESSING` immediately, and commits to free the database connection.
2. For each fetched event, serializes as a standard Kafka envelope using `correlationId` from the originating HTTP request's `X-Correlation-ID` (stored in outbox payload).
3. Uses `inventoryId` as the Kafka partition key to ensure all events for the same inventory bucket go to the same partition (guaranteeing correct event ordering).
4. On successful publish, performs a quick transaction to update the status to `PUBLISHED` (and sets `published_at`).
5. On failure, resets status back to `PENDING` (or marks as `FAILED` if `retry_count` exceeds limit).

---

## 8. Layered Architecture & File Map

```
services/flight-service/
│
├── prisma/
│   ├── schema.prisma              ← All models (Section 3.4)
│   ├── migrations/
│   │   └── 20260528_init/
│   │       └── migration.sql      ← includes CHECK constraint SQL
│   └── seed.ts                    ← Phase 1: airports+aircraft, Phase 2: schedules, Phase 3: instances
│
├── src/
│   │
│   ├── config/
│   │   ├── env.ts                 ← Zod-validated env vars — crash on startup if invalid
│   │   ├── database.ts            ← Prisma client singleton
│   │   ├── kafka.ts               ← KafkaJS producer (allowAutoTopicCreation: false)
│   │   └── logger.ts              ← Pino with AsyncLocalStorage for correlationId
│   │
│   ├── repositories/
│   │   ├── airport.repository.ts  ← Prisma airport queries
│   │   ├── aircraft.repository.ts ← Prisma aircraft queries
│   │   ├── flight.repository.ts   ← Prisma schedule + instance + inventory queries
│   │   └── outbox.repository.ts   ← Insert/query/update outbox_events
│   │
│   ├── services/
│   │   ├── airport.service.ts     ← Airport CRUD business logic
│   │   ├── aircraft.service.ts    ← Aircraft CRUD business logic
│   │   ├── flight.service.ts      ← Schedule + instance + inventory CRUD
│   │   └── inventory.service.ts   ← Seat hold/release with $transaction + FOR UPDATE
│   │
│   ├── controllers/
│   │   ├── airport.controller.ts  ← HTTP handlers for /api/v1/airports
│   │   ├── aircraft.controller.ts ← HTTP handlers for /api/v1/aircrafts
│   │   ├── flight.controller.ts   ← HTTP handlers for /api/v1/flights (schedules + instances + inventory)
│   │   └── internal.controller.ts ← HTTP handlers for /internal/flights (hold/release)
│   │
│   ├── routes/
│   │   ├── airport.routes.ts      ← /api/v1/airports
│   │   ├── aircraft.routes.ts     ← /api/v1/aircrafts
│   │   ├── flight.routes.ts       ← /api/v1/flights (schedules, instances, inventories)
│   │   ├── internal.routes.ts     ← /internal/flights (hold-seats, release-seats)
│   │   ├── health.routes.ts       ← GET /health
│   │   ├── metrics.routes.ts      ← GET /metrics
│   │   └── schemas/
│   │       └── flight.schemas.ts  ← All Zod schemas (Section 6)
│   │
│   ├── middlewares/
│   │   ├── requireRole.ts         ← Reads X-User-Role header, enforces minimum role
│   │   ├── internalAuth.ts        ← Validates X-Internal-Secret on /internal/* routes
│   │   ├── validate.ts            ← req.body Zod validation middleware factory
│   │   ├── validateQuery.ts       ← req.query Zod validation
│   │   ├── validateParams.ts      ← req.params Zod validation (UUID + IATA code checks)
│   │   └── errorHandler.ts        ← Global Express error handler
│   │
│   ├── events/
│   │   ├── producers/
│   │   │   └── flight.producer.ts ← Serializes + publishes Kafka envelope
│   │   └── outbox.worker.ts       ← setInterval: polls outbox → publishes → marks done
│   │
│   ├── types/
│   │   └── express.d.ts           ← Augments req: userId?, userRole?, validatedQuery?, validatedParams?
│   │
│   ├── utils/
│   │   └── response.utils.ts      ← sendSuccess(), sendError()
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
│       ├── airport.test.ts
│       ├── aircraft.test.ts
│       ├── flight.schedule.test.ts
│       ├── flight.instance.test.ts
│       ├── flight.inventory.test.ts
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

**`inventory.service.ts` is separate from `flight.service.ts`** because it uses `prisma.$queryRaw` for `SELECT ... FOR UPDATE` — Prisma's typed query builder does not support `FOR UPDATE`. Keeping these separate prevents mixing locking logic into general CRUD.

**`flight.producer.ts` is called by `outbox.worker.ts` only**, not by services directly. Services only write to the outbox table. If Kafka is down during a flight create, the HTTP response still returns 201 and the event publishes when Kafka recovers.

**`requireRole.ts`** reads `X-User-Id` and `X-User-Role` headers (injected by Gateway), validates presence, and checks the role. Attaches `req.userId` and `req.userRole`. `X-User-Id` is stored as `createdById` on new schedules and instances.

**Route mounting in `app.ts`:**
```typescript
app.use('/api/v1/airports',  airportRouter)
app.use('/api/v1/aircrafts', aircraftRouter)
app.use('/api/v1/flights',   flightRouter)
app.use('/internal/flights', internalAuth, internalRouter)
app.use('/',                 healthRouter)
app.use('/',                 metricsRouter)
app.use(globalErrorHandler)   // must be last
```

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
    "build":        "tsup src/server.ts --format esm --clean --minify",
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
    "tsup":                 "^8.4.0",
    "tsx":                  "^4.15.7",
    "vitest":               "^1.6.0"
  }
}
```

### Dependency Explanations

| Package | Why |
|---|---|
| `@prisma/client` | Type-safe PostgreSQL ORM. For `SELECT...FOR UPDATE`, use `prisma.$queryRaw` with tagged template literals (also parameterized — prevents SQL injection). |
| `kafkajs` | Producer only — publishes `flight-inventory-events`. No consumer in this service. |
| `prom-client` | Exposes `/metrics`. Custom counter for `flight_hold_requests_total{result}` and gauge for `outbox_pending_count`. |
| `uuid` | Generates `eventId` for Kafka message envelopes. |
| No `ioredis` | Flight Service does NOT use Redis. Cache invalidation is handled by the Search Service on Kafka event consumption. |
| No `bcrypt` | No passwords. No authentication logic. |
| No `jose` | No JWT signing. Relies on Gateway — reads trusted `X-User-Role` / `X-User-Id` headers only. |

---

## 10. Environment Variables

**File: `services/flight-service/.env.example`**

```bash
# ── Server ────────────────────────────────────────────────────────────
NODE_ENV=development
PORT=3002
SERVICE_NAME=flight-service

# ── Database (PostgreSQL via Prisma) ─────────────────────────────────
DATABASE_URL=postgresql://skyhub:skyhub_local@localhost:5432/skyhub_flight_db?connection_limit=10&pool_timeout=10

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=flight-service
KAFKA_TOPIC_FLIGHT_EVENTS=flight-inventory-events

# ── Outbox Worker ─────────────────────────────────────────────────────
OUTBOX_POLL_INTERVAL_MS=5000

# ── Internal Auth ─────────────────────────────────────────────────────
# Shared secret validated on every /internal/* request via X-Internal-Secret header
# Generate for production: openssl rand -hex 32
INTERNAL_SECRET=change-me-in-production-use-openssl-rand-hex-32

# ── Seat Hold ─────────────────────────────────────────────────────────
# Must match the BullMQ job delay configured in Booking Service
SEAT_HOLD_DURATION_MINUTES=15

# ── Observability ─────────────────────────────────────────────────────
LOG_LEVEL=info
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

### Env Validation Schema (Zod) — Key Rules

| Variable | Rule |
|---|---|
| `NODE_ENV` | enum: `development \| production \| test` |
| `PORT` | string → transform to number, default `3002` |
| `DATABASE_URL` | `z.string().url()` |
| `KAFKA_BROKERS` | `z.string()` — comma-separated, split in kafka.ts |
| `KAFKA_CLIENT_ID` | `z.string()` |
| `KAFKA_TOPIC_FLIGHT_EVENTS` | `z.string()` |
| `OUTBOX_POLL_INTERVAL_MS` | string → number, default `5000` |
| `INTERNAL_SECRET` | `z.string().min(32)` — crash on startup if missing or too short |
| `SEAT_HOLD_DURATION_MINUTES` | string → number, default `15` |
| `LOG_LEVEL` | enum: `error \| warn \| info \| debug`, default `info` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `z.string().url().optional()` |

---

## 11. Step-by-Step Build Plan

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

**Validation:** `npm run typecheck` → zero errors. `npm run dev` → crashes with Zod error if any env var is missing, else shows "port listening" log.

---

### Step 2: Database Migration

1. Ensure Postgres is running: `docker compose up -d`
2. Verify `skyhub_flight_db` database exists
3. Copy schema from Section 3.4
4. Run: `npx prisma migrate dev --name init`
5. Manually add CHECK constraints from Section 3.3 to the generated `migration.sql`.
6. Optimize the outbox database index in `migration.sql` by replacing the default compound status index with a high-performance partial index:
   ```sql
   DROP INDEX IF EXISTS outbox_events_status_created_at_idx;
   CREATE INDEX idx_pending_outbox ON outbox_events (created_at) WHERE status = 'PENDING';
   ```

**Seed file (`prisma/seed.ts`) — three phases in strict order (FK constraints require this):**

**Phase 1 — Reference data (airports + aircraft must exist before schedules):**
```typescript
await prisma.airport.createMany({
  data: [
    { id: 'airport-del-001', code: 'DEL', name: 'Indira Gandhi International', city: 'Delhi', country: 'India', timezone: 'Asia/Kolkata' },
    { id: 'airport-bom-001', code: 'BOM', name: 'Chhatrapati Shivaji Maharaj International', city: 'Mumbai', country: 'India', timezone: 'Asia/Kolkata' },
    { id: 'airport-blr-001', code: 'BLR', name: 'Kempegowda International', city: 'Bengaluru', country: 'India', timezone: 'Asia/Kolkata' },
    { id: 'airport-maa-001', code: 'MAA', name: 'Chennai International', city: 'Chennai', country: 'India', timezone: 'Asia/Kolkata' },
    { id: 'airport-hyd-001', code: 'HYD', name: 'Rajiv Gandhi International', city: 'Hyderabad', country: 'India', timezone: 'Asia/Kolkata' },
  ],
  skipDuplicates: true,
});

await prisma.aircraft.createMany({
  data: [
    { id: 'aircraft-a320-001', model: 'Airbus A320', totalCapacity: 180 },
    { id: 'aircraft-b737-001', model: 'Boeing 737-800', totalCapacity: 162 },
  ],
  skipDuplicates: true,
});
```

**Phase 2 — Flight schedules (depends on airports + aircraft):**
```typescript
const schedule = await prisma.flightSchedule.upsert({
  where: { uq_schedule_identity: { flightNumber: '6E-204', originAirportId: 'airport-del-001', destinationAirportId: 'airport-bom-001' } },
  create: {
    flightNumber: '6E-204', airline: 'IndiGo',
    originAirportId: 'airport-del-001', destinationAirportId: 'airport-bom-001',
    departureTime: '06:30', arrivalTime: '09:15', durationMinutes: 165,
    aircraftId: 'aircraft-a320-001', amenities: ['usb', 'snack'],
    createdById: 'seed-admin-id',
  },
  update: {},
});
```

**Phase 3 — Flight instances + seat inventories (depends on schedules):**
```typescript
await prisma.flightInstance.create({
  data: {
    scheduleId: schedule.id,
    departureDate: new Date('2026-10-12'),
    arrivalDate: new Date('2026-10-12'),
    createdById: 'seed-admin-id',
    inventories: {
      create: [
        { cabinClass: 'ECONOMY', fareClass: 'Y', basePrice: 499900, totalSeats: 150, availableSeats: 150,
          baggageAllowance: { cabin: '7kg', checked: '15kg' }, refundable: false },
        { cabinClass: 'BUSINESS', fareClass: 'C', basePrice: 1299900, totalSeats: 24, availableSeats: 24,
          baggageAllowance: { cabin: '10kg', checked: '25kg' }, refundable: true },
      ],
    },
  },
});
```

**Validation:** Run `npm run seed`. Verify with Prisma Studio: `airports` = 5 rows, `aircrafts` = 2 rows, `flight_schedules` rows with correct FK links, `seat_inventories` rows.

---

### Step 3: Utilities & Common Infrastructure

1. `src/config/database.ts` — Prisma client singleton
2. `src/config/kafka.ts` — KafkaJS producer with `allowAutoTopicCreation: false`
3. `src/config/logger.ts` — Pino with AsyncLocalStorage
4. `src/utils/response.utils.ts` — `sendSuccess()`, `sendError()`
5. `src/types/express.d.ts` — augment `Request` with `userId?`, `userRole?`, `validatedQuery?`, `validatedParams?`
6. `src/middlewares/requireRole.ts`:
```typescript
export function requireRole(...allowedRoles: string[]): RequestHandler
// Usage:
// router.post('/', requireRole('FLIGHT_ADMIN', 'SUPER_ADMIN'), controller.create)
// router.delete('/:id', requireRole('SUPER_ADMIN'), controller.cancel)
```
7. `src/middlewares/validate.ts`, `validateQuery.ts`, `validateParams.ts`
8. `src/middlewares/errorHandler.ts` — global error handler
9. `src/routes/schemas/flight.schemas.ts` from Section 6

---

### Step 4: Repository Layer

```
airport.repository.ts:
  findAll(filters): Promise<Airport[]>
  findById(id): Promise<Airport | null>
  findByCode(code): Promise<Airport | null>
  create(data): Promise<Airport>
  update(id, data): Promise<Airport>

aircraft.repository.ts:
  findAll(): Promise<Aircraft[]>
  findById(id): Promise<Aircraft | null>
  create(data): Promise<Aircraft>
  update(id, data): Promise<Aircraft>

flight.repository.ts:
  createSchedule(data): Promise<FlightSchedule>
  findScheduleById(id): Promise<FlightSchedule | null>
  findAllSchedules(filters, pagination): Promise<{ schedules, total }>
  updateSchedule(id, data): Promise<FlightSchedule>
  deleteSchedule(id): Promise<void>
  createInstance(data): Promise<FlightInstance>
  findInstanceById(id): Promise<FlightInstance | null>
  findAllInstances(filters, pagination): Promise<{ instances, total }>
  updateInstance(id, data): Promise<FlightInstance>
  cancelInstance(id): Promise<FlightInstance>
  updateInventory(inventoryId, data): Promise<SeatInventory>
  findInventoryById(inventoryId): Promise<SeatInventory | null>

outbox.repository.ts:
  create(tx, eventType, payload): Promise<void>   ← accepts Prisma tx for atomic writes
  fetchAndMarkProcessing(limit): Promise<OutboxEvent[]> ← uses FOR UPDATE SKIP LOCKED and sets status to PROCESSING in a short transaction
  markPublished(id): Promise<void>
  resetToPending(id): Promise<void>               ← resets status back to PENDING and increments retryCount
  markFailed(id): Promise<void>                   ← marks as FAILED after max retries exceeded
```

**Pagination pattern for list queries:**
```typescript
const [records, total] = await prisma.$transaction([
  prisma.flightInstance.findMany({ where, include: { schedule: true, inventories: true }, skip, take, orderBy }),
  prisma.flightInstance.count({ where }),
]);
return { records, total };
```

---

### Step 5: Service Layer

**`inventory.service.ts`** — `SELECT ... FOR UPDATE` using `prisma.$queryRaw`:

```
holdSeats(instanceId, cabinClass, fareClass, seats, bookingId):
  prisma.$transaction(async (tx) => {
    0. Idempotency Guard: check if outbox event with eventType = 'SEATS_HELD' and payload.bookingId = bookingId exists.
       If yes, return stored details directly.
    1. Raw query: SELECT id, available_seats, total_seats FROM seat_inventories
                  WHERE flight_instance_id = $1 AND cabin_class = $2::"CabinClass" AND fare_class = $3
                  FOR UPDATE
    2. If not found → throw 404
    3. Check flight instance status = SCHEDULED → throw 400 FLIGHT_NOT_ACTIVE if not
    4. If available_seats < seats → throw 409 INSUFFICIENT_SEATS
    5. tx.seatInventory.update({ decrement: { availableSeats: seats } })
    6. outboxRepo.create(tx, 'SEATS_HELD', { inventoryId, seatsHeld, remainingSeats, heldUntil, bookingId })
    7. return { remainingSeats, heldUntil }
  })

releaseSeats(instanceId, cabinClass, fareClass, seats, bookingId):
  prisma.$transaction(async (tx) => {
    0. Idempotency Guard: check if outbox event with eventType = 'SEATS_RELEASED' and payload.bookingId = bookingId exists.
       If yes, return stored details directly.
    1. findUnique by (flightInstanceId + cabinClass + fareClass) → 404 if null
    2. tx.$executeRaw`UPDATE seat_inventories
         SET available_seats = LEAST(available_seats + ${seats}, total_seats)
         WHERE id = ${inventory.id}`
    3. findUnique again → get updated availableSeats
    4. outboxRepo.create(tx, 'SEATS_RELEASED', { inventoryId, seatsReleased, remainingSeats, bookingId })
    5. return { remainingSeats }
  })
```

---

### Step 6: Controllers + Routes

**`src/app.ts` route mounting:**
```typescript
app.use('/api/v1/airports',  airportRouter)
app.use('/api/v1/aircrafts', aircraftRouter)
app.use('/api/v1/flights',   flightRouter)
app.use('/internal/flights', internalAuth, internalRouter)
app.use('/',                 healthRouter)
app.use('/',                 metricsRouter)
app.use(globalErrorHandler)
```

**`src/routes/flight.routes.ts`:**
```
POST   /schedules                              → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → createSchedule
GET    /schedules                              → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → listSchedules
GET    /schedules/:id                          → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → getScheduleById
PATCH  /schedules/:id                          → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → updateSchedule
DELETE /schedules/:id                          → requireRole(SUPER_ADMIN)               → deleteSchedule
POST   /instances                              → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → createInstance
GET    /instances                              → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → listInstances
GET    /instances/:id                          → (no auth) → getInstanceById
PATCH  /instances/:id                          → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → updateInstance
DELETE /instances/:id                          → requireRole(SUPER_ADMIN)               → cancelInstance
PATCH  /instances/:instanceId/inventories/:id  → requireRole(FLIGHT_ADMIN, SUPER_ADMIN) → updateInventory
```

**`src/routes/internal.routes.ts`:**
```
PATCH  /instances/:id/hold-seats    → holdSeats
PATCH  /instances/:id/release-seats → releaseSeats
```

**Validation:** Full CRUD via Postman. Seed airports/aircraft first → create schedule → create instance → GET public detail → hold seats → update inventory → cancel instance. Verify 403 on role mismatch, 404 on missing FK.

---

### Step 7: Outbox Worker + Kafka Producer

1. `src/events/producers/flight.producer.ts`:
   - Takes `OutboxEvent` from DB
   - Builds standard Kafka envelope
   - Publishes with `inventoryId` from `payload.inventoryId` as the message key
   - Returns success/failure boolean

2. `src/events/outbox.worker.ts`:
   - Exported `startOutboxWorker()` starts a recursive `setTimeout` loop
   - Reads `env.OUTBOX_POLL_INTERVAL_MS` for delay
   - Calls `outboxRepo.getPending(100)` (using `FOR UPDATE SKIP LOCKED` inside raw transaction polling) each tick
   - For each event: `flightProducer.publish(event)` → marks PUBLISHED or FAILED
   - Worker must catch all errors and survive — never rethrow

3. Update `server.ts` to call `startOutboxWorker()` after Kafka producer connects

**End-to-end test:**
- Create a flight instance via POST
- Verify `outbox_events` row appears with `status='PENDING'`
- Wait 5s → verify `status='PUBLISHED'`
- Use Kafka UI to confirm message landed in `flight-inventory-events`

---

### Step 8: Health, Metrics, server.ts

```
bootstrap():
  1. await prisma.$connect()
  2. await kafkaProducer.connect()
  3. startOutboxWorker()
  4. const server = app.listen(env.PORT)
  5. process.on('SIGTERM', shutdown)
  6. process.on('SIGINT',  shutdown)

shutdown(signal):
  1. server.close()
  2. kafkaProducer.disconnect()   ← flushes pending messages
  3. prisma.$disconnect()         ← closes connection pool
  4. process.exit(0)
```

**Full validation:**
```bash
curl http://localhost:3002/health
# Expected: { "status": "healthy", "checks": { "database": "ok", "kafka": "ok" } }

# Test hold-seats internal endpoint
curl -X PATCH http://localhost:3002/internal/flights/instances/{instanceId}/hold-seats \
  -H "Content-Type: application/json" \
  -H "X-Internal-Secret: change-me-in-production-use-openssl-rand-hex-32" \
  -d '{"seats": 2, "cabinClass": "ECONOMY", "fareClass": "Y", "bookingId": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}'
# Expected: { "success": true, "remainingSeats": 148, "heldUntil": "..." }
```

---

## 12. Testing Strategy

### Unit Tests

**`tests/unit/services/inventory.service.test.ts`** — the most important test file:

Mock Prisma client using `vitest.mock()`. Test the logic without a real database:

| Test Case | What to Verify |
|---|---|
| Hold succeeds with enough seats | `availableSeats` decremented, outbox event inserted, `remainingSeats` returned |
| Hold fails — insufficient seats | 409 thrown with `availableSeats` in details |
| Hold fails — flight not SCHEDULED | 400 FLIGHT_NOT_ACTIVE thrown |
| Hold fails — inventory bucket not found | 404 thrown |
| Hold is idempotent (repeated call) | Query returns cached payload directly without decrementing available seats |
| Release increments seats | `availableSeats` incremented up to `totalSeats` cap |
| Release is idempotent (double call) | Query returns cached payload directly without incrementing available seats again |
| Concurrent holds (simulate race) | Both requests processed sequentially via lock; second sees updated seat count |

### Integration Tests

**`tests/integration/flight.hold.test.ts`** — must use a real PostgreSQL test database:

```typescript
it('concurrent holds on last 2 seats — only one succeeds', async () => {
  // Create a flight instance with availableSeats = 2
  const [res1, res2] = await Promise.all([
    request(app).patch(`/internal/flights/instances/${instanceId}/hold-seats`)
      .set('X-Internal-Secret', env.INTERNAL_SECRET)
      .send({ seats: 2, cabinClass: 'ECONOMY', fareClass: 'Y', bookingId: uuid() }),
    request(app).patch(`/internal/flights/instances/${instanceId}/hold-seats`)
      .set('X-Internal-Secret', env.INTERNAL_SECRET)
      .send({ seats: 2, cabinClass: 'ECONOMY', fareClass: 'Y', bookingId: uuid() }),
  ]);
  const statuses = [res1.status, res2.status].sort();
  expect(statuses).toEqual([200, 409]);   // exactly one success, one conflict
});
```

This test is the most important integration test in the entire service — it verifies the `SELECT ... FOR UPDATE` lock actually works under concurrency.

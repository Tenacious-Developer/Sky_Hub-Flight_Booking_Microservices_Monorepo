# SkyHub — Booking Service: Complete Production-Grade Build Guide

## Table of Contents

1. [Bounded Context & Responsibility](#1-bounded-context--responsibility)
2. [Complete Feature List](#2-complete-feature-list)
3. [Database Design & Prisma Schema](#3-database-design--prisma-schema)
4. [Saga State Machine](#4-saga-state-machine)
5. [Complete REST API Specification](#5-complete-rest-api-specification)
6. [Zod Validation Schemas](#6-zod-validation-schemas)
7. [Message Queue Architecture](#7-message-queue-architecture)
8. [External HTTP Calls (Flight Service)](#8-external-http-calls-flight-service)
9. [Layered Architecture & File Map](#9-layered-architecture--file-map)
10. [npm Dependencies](#10-npm-dependencies)
11. [Environment Variables](#11-environment-variables)
12. [Step-by-Step Build Plan](#12-step-by-step-build-plan)
13. [Testing Strategy](#13-testing-strategy)

---

## 1. Bounded Context & Responsibility

The Booking Service is the **Saga Orchestrator** for the entire checkout transaction. It owns the booking lifecycle end-to-end — from the moment a user clicks "Book" to the moment the booking is CONFIRMED or CANCELLED. It drives every saga step, coordinates with Flight Service (seat hold) and Payment Service (via RabbitMQ), and handles all rollback paths.

```
COMPLETE BOOKING FLOW — ALL PATHS

HAPPY PATH:
  CLIENT → POST /api/v1/bookings
                │
       BOOKING SERVICE
                │
    ┌─── SAGA STEP 1 ───┐
    │  HTTP → Flight     │  GET flight price
    │  HTTP → Flight     │  PATCH hold-seats (FOR UPDATE lock)
    │  If 409 → return   │  → 409 Conflict to client (no seats)
    └───────────────────┘
                │
    ┌─── SAGA STEP 2 ───┐
    │  DB TRANSACTION:  │
    │  INSERT booking   │  status = PENDING_PAYMENT
    │  INSERT saga_log  │  state  = SEAT_HELD
    │  INSERT outbox    │  type   = BOOKING_INITIATED (→ RabbitMQ)
    └───────────────────┘
                │
    ┌─── SAGA STEP 3 ───┐
    │  BullMQ delayed   │  seat-timeout-queue, delay=15min
    │  job scheduled    │  jobId = bookingId (deterministic)
    └───────────────────┘
                │
       Return 201 { bookingId, totalAmount, heldUntil }

  [Background] Outbox Worker → BOOKING_INITIATED → RabbitMQ

  CLIENT pays via Stripe.js → Stripe webhook → PAYMENT SERVICE
  PAYMENT SERVICE → PAYMENT_SUCCESS to RabbitMQ payment.result

  BOOKING SERVICE (RabbitMQ consumer)
       │  idempotency check: status already CONFIRMED? skip
       ├─ UPDATE booking → CONFIRMED
       ├─ UPDATE saga_log → COMPLETED
       ├─ INSERT outbox (BOOKING_COMPLETED → Kafka booking-events)
       ├─ BullMQ: remove seat-timeout job (no longer needed)
       ├─ BullMQ: add email-queue job { bookingId, type: CONFIRMED }
       └─ BullMQ: add reminder-queue job { bookingId } delay=24h before departure

─────────────────────────────────────────────────────────────

PAYMENT FAILED PATH:
  PAYMENT SERVICE → PAYMENT_FAILED to RabbitMQ payment.result

  BOOKING SERVICE (RabbitMQ consumer)
       │  idempotency check: status already CANCELLED? skip
       ├─ UPDATE booking → CANCELLED
       ├─ UPDATE saga_log → ROLLBACK_INITIATED
       ├─ INSERT outbox (RELEASE_SEATS → Flight Service HTTP)
       └─ Outbox Worker → HTTP PATCH /internal/flights/:id/release-seats (retry x5)
          On success → saga_log → ROLLBACK_COMPLETED
          On all fail → saga_log → ROLLBACK_FAILED + ops alert (DLQ)

─────────────────────────────────────────────────────────────

SEAT TIMEOUT PATH (user abandoned payment):
  BullMQ seat-timeout-queue fires after 15 minutes

  BOOKING SERVICE (BullMQ worker)
       │  idempotency: status still PENDING_PAYMENT?
       │  NO  → skip (booking was already confirmed or cancelled)
       ├─ UPDATE booking → TIMED_OUT
       ├─ UPDATE saga_log → TIMED_OUT
       ├─ HTTP PATCH /internal/flights/:id/release-seats
       └─ BullMQ: add email-queue job { bookingId, type: BOOKING_EXPIRED }
```

**Hard boundaries — what this service owns and what it does not touch:**

| Owns | Does NOT own |
|---|---|
| `skyhub_booking_db` (PostgreSQL, exclusive) | Any other service's database |
| Booking lifecycle (PENDING → CONFIRMED / CANCELLED / TIMED_OUT) | Payment processing (Payment Service owns it) |
| Saga state machine (`saga_logs` table) | Seat inventory (Flight Service owns it) |
| BullMQ job scheduling (seat-timeout, email, reminder) | Email sending (Notification Service owns it) |
| `booking-events` Kafka topic (producer) | User loyalty tier (User Service owns it) |
| RabbitMQ `booking.initiated` queue (producer) | Flight pricing source of truth |
| RabbitMQ `payment.result` queue (consumer) | |

**Data contract with other services:**

| Service | How they interact |
|---|---|
| **Flight Service** | Synchronous HTTP: GET price, PATCH hold-seats, PATCH release-seats |
| **Payment Service** | Async RabbitMQ: Booking produces `BOOKING_INITIATED`, consumes `PAYMENT_RESULT` |
| **Notification Service** | BullMQ jobs on Redis DB 3: email-queue, reminder-queue jobs. Notification Service calls back `GET /internal/bookings/:id` to fetch booking details for the email |
| **User Service** | Kafka: Booking produces `BOOKING_COMPLETED` to `booking-events` topic → User Service increments `booking_count` for loyalty tier upgrade |

---

## 2. Complete Feature List

### Feature 1: Create Booking (Saga Initiation)

**Who can call:** Any authenticated user (`CUSTOMER`, `FLIGHT_ADMIN`, `SUPER_ADMIN`)

**Design Decision: Synchronous Hold vs. Asynchronous Event-Driven Hold**
We use a **Synchronous HTTP API Call** to the Flight Service to hold seats during booking initiation, rather than an asynchronous event-driven message. This ensures the user gets immediate, real-time feedback on seat availability at checkout. In contrast, an asynchronous hold would require a loading spinner and WebSocket notification, resulting in poor user experience if the flight sells out.

**Step 1 — Fetch Price + Hold Seats (two HTTP calls to Flight Service):**

```
1a. GET http://flight-service:3002/api/v1/flights/{flightId}
    → Retrieve basePrice and verify flight is ACTIVE
    → If 404 → return 404 to client ("Flight not found")
    → If status ≠ ACTIVE → return 422 ("Flight is not available for booking")

1b. Apply loyalty discount (from X-User-Loyalty-Tier header):
    discountedPricePerSeat = Math.round(basePrice * multiplier)
    totalAmount = discountedPricePerSeat * seats

1c. PATCH http://flight-service:3002/internal/flights/{flightId}/hold-seats
    Body: { seats, bookingId: pre-generated UUID }
    
    Database Locking (inside Flight Service):
      - Executes a SELECT ... FOR UPDATE row-level pessimistic lock on the flight record.
      - Decrements available seats safely within an ACID PostgreSQL transaction.
      - Returns heldUntil timestamp (e.g., NOW + 15 minutes).
      
    Result Resolution & Network Reliability:
      → 200 { remainingSeats, heldUntil } → proceed
      → 409 INSUFFICIENT_SEATS → return 409 to client immediately
      → 5xx / network error → Handled by Opossum Circuit Breaker (opens after 3 failures, returns 503)
      → Transient Network Blips → Handled by Axios-Retry (3 retries with exponential backoff: 1s, 2s, 4s)
```

**Step 2 — Atomic DB Write (all-or-nothing in one Prisma transaction):**
```
BEGIN TRANSACTION
  INSERT INTO bookings (
    id = pre-generated UUID,
    userId, flightId, seats, status = PENDING_PAYMENT,
    totalAmount, currency = 'INR',
    heldUntil,           ← from hold-seats response
    passengerDetails,
    contactEmail, contactPhone
  )
  INSERT INTO saga_logs (bookingId, state = SEAT_HELD)
  INSERT INTO outbox_events (
    eventType = 'BOOKING_INITIATED',
    destination = RABBITMQ,
    payload = { bookingId, userId, flightId, seats, totalAmount, currency }
  )
COMMIT
```

Why all three writes in one transaction? If the service crashes after inserting the booking but before inserting the outbox event, the Payment Service would never receive the `BOOKING_INITIATED` message — payment page would never load. The atomic write guarantees this cannot happen.

**Step 3 — Schedule Seat Hold Expiry:**
```
BullMQ.add('seat-timeout-queue', {
  data:  { bookingId, flightId, seats },
  opts:  { delay: 15 * 60 * 1000, jobId: bookingId }
})
```

The `jobId = bookingId` (deterministic) is critical. If the service restarts and the API call is retried, the second `BullMQ.add` with the same `jobId` is a no-op — BullMQ rejects duplicate job IDs silently. This prevents double-timeout cancellation.

**Return:** `201 Created { bookingId, totalAmount, currency, heldUntil, status: 'PENDING_PAYMENT' }`

---

### Feature 2: Payment Success Handler (RabbitMQ Consumer)

**Triggered by:** RabbitMQ message on `payment.result` queue, event type `PAYMENT_SUCCESS`

**Idempotency guard — always check status first:**
```
booking = SELECT FROM bookings WHERE id = bookingId
IF booking.status === 'CONFIRMED'  → ack message, return (already processed)
IF booking.status === 'CANCELLED'  → ack message, return (payment arrived after cancellation)
IF booking.status === 'TIMED_OUT'  → should not happen but: ack, log warning, return
```

**Processing (only if status = PENDING_PAYMENT):**
```
BEGIN TRANSACTION
  UPDATE bookings SET status = 'CONFIRMED', confirmedAt = NOW()
  UPDATE saga_logs SET state = 'COMPLETED' WHERE bookingId = ? AND state = 'SEAT_HELD'
  INSERT INTO outbox_events (
    eventType = 'BOOKING_COMPLETED',
    destination = KAFKA,
    payload = { bookingId, userId, flightId, seats, totalAmount }
  )
COMMIT

BullMQ.remove('seat-timeout-queue', bookingId)  ← cancel the timeout job

BullMQ.add('email-queue', {
  data: { bookingId, emailType: 'BOOKING_CONFIRMED' }
})

BullMQ.add('reminder-queue', {
  data: { bookingId },
  opts: { delay: reminderAt - Date.now(), jobId: `reminder:${bookingId}` },
  ← reminderAt = flight departureDateTime - 24 hours
})
```

**How to calculate reminder delay:**
The Booking Service needs to know the flight's departure datetime. This was stored at booking creation time (in `passengerDetails` or in a `flightDepartureAt` column). Design decision: store `flightDepartureAt` as a column on the `bookings` table at creation time — fetched from the Flight Service GET call in Feature 1. This avoids another HTTP call when processing the payment success event.

---

### Feature 3: Payment Failed Handler (Saga Rollback)

**Triggered by:** RabbitMQ message on `payment.result` queue, event type `PAYMENT_FAILED`

**Idempotency guard:**
```
IF booking.status === 'CANCELLED' → ack, return (already rolled back)
IF booking.status === 'CONFIRMED' → ack, log warning, return (edge case: success arrived first)
```

**Processing:**
```
BEGIN TRANSACTION
  UPDATE bookings SET status = 'CANCELLED', cancelledAt = NOW()
  INSERT INTO saga_logs (state = 'ROLLBACK_INITIATED')
  INSERT INTO outbox_events (
    eventType = 'RELEASE_SEATS',
    destination = HTTP,   ← not Kafka/RabbitMQ — HTTP call to Flight Service
    payload = { flightId, seats, bookingId }
  )
COMMIT
```

**The `RELEASE_SEATS` outbox event is special:**
Unlike other outbox events that go to a message broker, this one drives a direct HTTP call to Flight Service. The outbox worker handles it differently: instead of publishing to RabbitMQ/Kafka, it calls `PATCH /internal/flights/:id/release-seats` with exponential backoff retry (5 retries over ~10 minutes). On success, updates `saga_logs` to `ROLLBACK_COMPLETED`. On all failures, moves outbox to `FAILED` status and publishes an alert job to BullMQ for ops team (DLQ pattern).

---

### Feature 4: Seat Hold Timeout (BullMQ Worker)

**Triggered by:** BullMQ `seat-timeout-queue` job firing after 15-minute delay

**Idempotency guard (most important check):**
```
booking = SELECT FROM bookings WHERE id = bookingId
IF booking.status !== 'PENDING_PAYMENT' → skip (do nothing, job completes successfully)
```
This handles the case where payment was confirmed or cancelled between when the job was created and when it fires.

**Processing (only if still PENDING_PAYMENT):**
```
BEGIN TRANSACTION
  UPDATE bookings SET status = 'TIMED_OUT'
  INSERT INTO saga_logs (state = 'TIMED_OUT')
COMMIT

HTTP PATCH /internal/flights/:flightId/release-seats
  Body: { seats, bookingId }
  On failure: retry with axiosRetry (3 attempts, exponential delay)
  On total failure: log critical error, manual ops intervention needed

BullMQ.add('email-queue', {
  data: { bookingId, emailType: 'BOOKING_EXPIRED' }
})
```

---

### Feature 5: Get Booking by ID

**Who can call:** Authenticated users. A `CUSTOMER` can only fetch their own booking. `FLIGHT_ADMIN` and `SUPER_ADMIN` can fetch any booking.

**Ownership check:**
```
IF req.userRole === 'CUSTOMER' AND booking.userId !== req.userId → 403 Forbidden
```

**Response:** Full booking detail including saga log history (useful for debugging booking state)

---

### Feature 6: List User's Bookings

**Who can call:** Authenticated users. Returns only the calling user's bookings.

**SUPER_ADMIN exception:** If role is `SUPER_ADMIN`, a `userId` query param can be provided to list any user's bookings (admin support tool).

**Supported query filters:** `status`, `page`, `limit`

---

### Feature 7: User-Initiated Booking Cancellation

**Who can call:** Authenticated user (can only cancel their own bookings)

**Rules:**
- `PENDING_PAYMENT` → can cancel: update to `CANCELLED`, release seats via HTTP call to Flight Service, add `email-queue` job with type `BOOKING_CANCELLED_BY_USER`
- `CONFIRMED` → return 422 with message: `"Confirmed bookings cannot be cancelled through this endpoint. Please contact support for a refund."` (full refund flow is Phase 5 / Payment Service scope)
- `CANCELLED` / `TIMED_OUT` → return 400 `"Booking is already inactive"`

**On cancel of PENDING_PAYMENT:**
```
BEGIN TRANSACTION
  UPDATE bookings SET status = 'CANCELLED', cancelledAt = NOW()
  INSERT INTO saga_logs (state = 'ROLLBACK_INITIATED')
COMMIT

HTTP PATCH /internal/flights/:flightId/release-seats (with retry)
  On success → INSERT saga_logs (state = 'ROLLBACK_COMPLETED')

BullMQ.remove('seat-timeout-queue', bookingId)   ← cancel the pending timeout job
BullMQ.add('email-queue', { bookingId, emailType: 'BOOKING_CANCELLED_BY_USER' })
```

---

### Feature 8: Internal — Get Booking Detail (for Notification Service)

**Path:** `GET /internal/bookings/:id`
**Who can call:** Only Notification Service (internal network, not proxied by Gateway)

Notification Service's BullMQ workers receive only `{ bookingId }` in job data (no PII stored in Redis). When processing an email job, Notification Service calls this endpoint to fetch the full booking with flight info and passenger details needed to generate the PDF ticket.

**Response includes:** booking fields + `flightInfo` (fetched from Flight Service by Booking Service, or cached on booking creation), passenger details, contact email — everything Notification Service needs for the PDF.

---

### Feature 9: Outbox Worker (Background)

The Outbox Worker handles three types of outbox events:

| `destination` | `eventType` | Action |
|---|---|---|
| `RABBITMQ` | `BOOKING_INITIATED` | Publish to RabbitMQ exchange `skyhub.booking`, routing key `booking.initiated` |
| `KAFKA` | `BOOKING_COMPLETED` | Publish to Kafka topic `booking-events`, key = `userId` |
| `HTTP` | `RELEASE_SEATS` | HTTP PATCH to Flight Service `/internal/flights/:id/release-seats` with retry |

Each type is handled by a separate publisher function called from the same outbox polling loop. The `destination` field routes to the correct publisher.

---

### Feature 10: Health Check + Metrics

**`GET /health`:** Checks PostgreSQL, RabbitMQ consumer connection, Kafka producer connection, BullMQ Redis connection (DB 3)

**`GET /metrics`:** Prometheus format. Booking Service-specific metrics:
- `saga_state_transitions_total{state}` — counter per saga state (SEAT_HELD, COMPLETED, ROLLBACK_INITIATED, etc.)
- `active_pending_bookings` — gauge: count of bookings in PENDING_PAYMENT status (alert if grows abnormally)
- `seat_timeout_jobs_total` — counter: how many seat timeouts fired (users abandoning payment)
- `booking_created_total` — counter
- `booking_confirmed_total` — counter
- `booking_cancelled_total{reason}` — counter: `reason=payment_failed|user_cancelled|timed_out`

---

## 3. Database Design & Prisma Schema

### 3.1 Entity-Relationship Diagram

```
┌───────────────────────────────────────────────────────────────┐
│                          BOOKINGS                             │
├───────────────────────────────────────────────────────────────┤
│ id                  UUID         PK                           │
│ user_id             UUID         NOT NULL  (from JWT — no FK) │
│ flight_id           UUID         NOT NULL  (from Flight Svc)  │
│ seats               INT          NOT NULL                     │
│ status              ENUM         DEFAULT PENDING_PAYMENT      │
│ total_amount        INT          NOT NULL  (paise)            │
│ currency            VARCHAR(3)   DEFAULT 'INR'                │
│ held_until          TIMESTAMPTZ  NOT NULL                     │
│ flight_departure_at TIMESTAMPTZ  NOT NULL  (stored at create) │
│ passenger_details   JSONB        NOT NULL                     │
│ contact_email       VARCHAR(255) NOT NULL                     │
│ contact_phone       VARCHAR(20)  NULL                         │
│ confirmed_at        TIMESTAMPTZ  NULL                         │
│ cancelled_at        TIMESTAMPTZ  NULL                         │
│ created_at          TIMESTAMPTZ  DEFAULT NOW()                │
│ updated_at          TIMESTAMPTZ  AUTO UPDATE                  │
└──────────────────────────────┬────────────────────────────────┘
                               │ 1
                               │ has many
                               │ N
┌──────────────────────────────▼────────────────────────────────┐
│                          SAGA_LOGS                            │
├───────────────────────────────────────────────────────────────┤
│ id          UUID         PK                                   │
│ booking_id  UUID         FK → bookings.id ON DELETE CASCADE   │
│ state       ENUM         NOT NULL                             │
│ metadata    JSONB        NULL  (extra context per transition) │
│ created_at  TIMESTAMPTZ  DEFAULT NOW()                        │
└───────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────┐
│                        OUTBOX_EVENTS                          │
├───────────────────────────────────────────────────────────────┤
│ id           UUID         PK                                  │
│ event_type   VARCHAR(100) NOT NULL                            │
│ destination  ENUM         NOT NULL  (RABBITMQ|KAFKA|HTTP)     │
│ payload      JSONB        NOT NULL                            │
│ status       ENUM         DEFAULT PENDING                     │
│ attempts     INT          DEFAULT 0  (retry count for HTTP)   │
│ created_at   TIMESTAMPTZ  DEFAULT NOW()                       │
│ published_at TIMESTAMPTZ  NULL                                │
└───────────────────────────────────────────────────────────────┘
```

### 3.2 Column-by-Column Justification

#### `bookings` table

| Column | Type | Why This Design |
|---|---|---|
| `user_id` | UUID (no FK) | Booking DB does not have a `users` table — that lives in User Service. The UUID is trusted from the `X-User-Id` header (Gateway-verified). No FK = no cross-service DB dependency. |
| `flight_id` | UUID (no FK) | Same reasoning. The flight UUID is provided by the client and verified via the Flight Service GET call during booking creation. |
| `seats` | INT | Total seats booked across all passengers. Max 9 (matches search service validation). |
| `total_amount` | INT | Minor units (paise). Computed at booking creation: `discountedPricePerSeat × seats`. **Never recomputed after creation** — price is locked at booking time. |
| `held_until` | TIMESTAMPTZ | The expiry time from the Flight Service hold-seats response (`NOW() + 15 min`). Stored for display to the user ("Complete payment by 10:15 AM"). Also used as a reference if the BullMQ job is somehow lost — a recovery cron could check for bookings where `held_until < NOW()` and `status = PENDING_PAYMENT`. |
| `flight_departure_at` | TIMESTAMPTZ | Fetched from Flight Service at booking creation and stored here. Needed to calculate the reminder job delay (`departureAt - 24h`) when processing PAYMENT_SUCCESS — avoids a second HTTP call to Flight Service at that time. |
| `passenger_details` | JSONB | Array: `[{ name, type: ADULT|CHILD|INFANT }]`. JSONB allows adding fields (passport number, seat preference) in future without schema migration. Length must equal `seats`. |
| `contact_email` | VARCHAR(255) | Used by Notification Service to send confirmation email. Stored here because the Notification Service fetches booking data from this service, not User Service. |
| `confirmed_at` | TIMESTAMPTZ NULL | Set when status transitions to CONFIRMED. Used for analytics ("average time from booking creation to payment"). |
| `cancelled_at` | TIMESTAMPTZ NULL | Set when status transitions to CANCELLED or TIMED_OUT. Null if still active. |

#### `saga_logs` table

| Column | Type | Why |
|---|---|---|
| `booking_id` | UUID FK | Cascades on delete — if a booking is hard-deleted (admin cleanup), its saga log is also deleted. In practice, bookings are never hard-deleted. |
| `state` | ENUM | Every state transition creates a NEW row (append-only). No updates. This gives a full audit trail of the saga's history. |
| `metadata` | JSONB NULL | State-specific context. Examples: `SEAT_HOLD_FAILED: { reason: 'INSUFFICIENT_SEATS', availableSeats: 1 }`, `ROLLBACK_FAILED: { attempts: 5, lastError: '...' }`. Invaluable for debugging. |

**Why append-only saga_logs?**
If you update a single `state` column in `bookings`, you lose the history. With append-only rows: you can always see "SEAT_HELD at 10:00, PAYMENT_FAILED at 10:12, ROLLBACK_INITIATED at 10:12, ROLLBACK_COMPLETED at 10:13". This turns debugging from impossible to trivial.

#### `outbox_events` table

| Column | Type | Why |
|---|---|---|
| `destination` | ENUM: RABBITMQ, KAFKA, HTTP | The outbox worker routes to different publishers based on this field. Having all three event types in one table simplifies the polling query and avoids managing three separate outbox tables. |
| `attempts` | INT DEFAULT 0 | Incremented on each publish attempt. Used for retry limiting on HTTP events (max 5 attempts → FAILED status). RabbitMQ/Kafka events retry indefinitely until success (message broker is eventually available). |

### 3.3 Complete Prisma Schema

**File: `services/booking-service/prisma/schema.prisma`**

```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

// ─── Enums ────────────────────────────────────────────────────────────────────

enum BookingStatus {
  PENDING_PAYMENT
  CONFIRMED
  CANCELLED
  TIMED_OUT
}

enum SagaState {
  STARTED
  SEAT_HELD
  SEAT_HOLD_FAILED
  PAYMENT_SUCCESS
  PAYMENT_FAILED
  TIMED_OUT
  COMPLETED
  ROLLBACK_INITIATED
  ROLLBACK_COMPLETED
  ROLLBACK_FAILED
}

enum OutboxStatus {
  PENDING
  PUBLISHED
  FAILED
}

enum OutboxDestination {
  RABBITMQ
  KAFKA
  HTTP
}

// ─── Models ───────────────────────────────────────────────────────────────────

model Booking {
  id                String        @id @default(uuid())
  userId            String        @map("user_id")
  flightId          String        @map("flight_id")
  seats             Int
  status            BookingStatus @default(PENDING_PAYMENT)
  totalAmount       Int           @map("total_amount")
  currency          String        @default("INR")
  heldUntil         DateTime      @map("held_until")
  flightDepartureAt DateTime      @map("flight_departure_at")
  passengerDetails  Json          @map("passenger_details")
  contactEmail      String        @map("contact_email")
  contactPhone      String?       @map("contact_phone")
  confirmedAt       DateTime?     @map("confirmed_at")
  cancelledAt       DateTime?     @map("cancelled_at")
  createdAt         DateTime      @default(now()) @map("created_at")
  updatedAt         DateTime      @updatedAt @map("updated_at")

  sagaLogs SagaLog[]

  @@index([userId])
  @@index([flightId])
  @@index([status])
  @@index([userId, status])
  @@index([heldUntil, status])
  @@map("bookings")
}

model SagaLog {
  id        String    @id @default(uuid())
  bookingId String    @map("booking_id")
  state     SagaState
  metadata  Json?
  createdAt DateTime  @default(now()) @map("created_at")

  booking Booking @relation(fields: [bookingId], references: [id], onDelete: Cascade)

  @@index([bookingId])
  @@map("saga_logs")
}

model OutboxEvent {
  id          String            @id @default(uuid())
  eventType   String            @map("event_type")
  destination OutboxDestination
  payload     Json
  status      OutboxStatus      @default(PENDING)
  attempts    Int               @default(0)
  createdAt   DateTime          @default(now()) @map("created_at")
  publishedAt DateTime?         @map("published_at")

  @@index([status, createdAt])
  @@index([destination, status])
  @@map("outbox_events")
}
```

### 3.4 Index Summary

| Index | Columns | Purpose |
|---|---|---|
| `idx_booking_userId` | `userId` | List user's bookings |
| `idx_booking_flightId` | `flightId` | Admin lookup: all bookings for a flight |
| `idx_booking_status` | `status` | Filter by status, ops queries |
| `idx_booking_userId_status` | `(userId, status)` | User's bookings filtered by status |
| `idx_booking_heldUntil_status` | `(heldUntil, status)` | Recovery cron: find `PENDING_PAYMENT` bookings past `heldUntil` |
| `idx_sagaLog_bookingId` | `bookingId` | Fetch saga history for a booking |
| `idx_outbox_status_createdAt` | `(status, createdAt)` | Outbox Worker polling query |
| `idx_outbox_destination_status` | `(destination, status)` | Route-specific outbox queries |

**Why `(heldUntil, status)` index?**
A recovery cron job (or admin tool) can query: `WHERE status = 'PENDING_PAYMENT' AND held_until < NOW()` to find bookings where the BullMQ timeout job was somehow lost (e.g., Redis data loss). This index makes that query fast instead of a full table scan.

---

## 4. Saga State Machine

### 4.1 Visual State Machine

```
                    ┌──────────────┐
                    │   STARTED    │ ← booking creation begins
                    └──────┬───────┘
               ┌───────────┴───────────┐
               ▼                       ▼
    ┌──────────────────┐    ┌──────────────────┐
    │ SEAT_HOLD_FAILED │    │   SEAT_HELD      │ ← seat hold success
    │ (terminal)       │    └──────┬───────────┘
    │ booking never    │           │
    │ created          │    ┌──────┴──────────────────────┐
    └──────────────────┘    │                             │
                            ▼                             ▼
                   ┌──────────────┐             ┌──────────────────┐
                   │  TIMED_OUT   │             │  PAYMENT_SUCCESS │
                   │ (terminal)   │             └──────┬───────────┘
                   │ seats        │                    ▼
                   │ released     │          ┌──────────────────┐
                   └──────────────┘          │   COMPLETED      │ ← final happy state
                                             └──────────────────┘

                   ┌──────────────────┐
                   │  PAYMENT_FAILED  │
                   └──────┬───────────┘
                          ▼
                   ┌──────────────────────┐
                   │  ROLLBACK_INITIATED  │
                   └──────┬───────────────┘
              ┌───────────┴─────────────┐
              ▼                         ▼
    ┌──────────────────────┐   ┌──────────────────────┐
    │  ROLLBACK_COMPLETED  │   │   ROLLBACK_FAILED    │
    │ (terminal, seats     │   │ (ops alert — manual  │
    │  released)           │   │  intervention needed)│
    └──────────────────────┘   └──────────────────────┘
```

### 4.2 State Transition Rules

| From State | Event | To State | Action |
|---|---|---|---|
| — | Booking created | `STARTED` | Pre-transition: no DB write yet |
| `STARTED` | Hold-seats success | `SEAT_HELD` | Create booking + saga_log in transaction |
| `STARTED` | Hold-seats fails (409) | `SEAT_HOLD_FAILED` | Return 409 to client, no booking created |
| `SEAT_HELD` | BullMQ timer fires, still PENDING | `TIMED_OUT` | Release seats via HTTP |
| `SEAT_HELD` | RabbitMQ: PAYMENT_SUCCESS | `PAYMENT_SUCCESS` → `COMPLETED` | Confirm booking, schedule emails |
| `SEAT_HELD` | RabbitMQ: PAYMENT_FAILED | `PAYMENT_FAILED` → `ROLLBACK_INITIATED` | Cancel booking, release seats via outbox |
| `SEAT_HELD` | User cancels booking | `ROLLBACK_INITIATED` | Release seats, cancel timeout job |
| `ROLLBACK_INITIATED` | Seat release HTTP success | `ROLLBACK_COMPLETED` | Update saga_log |
| `ROLLBACK_INITIATED` | Seat release HTTP fails (all retries) | `ROLLBACK_FAILED` | Alert ops team, seats stuck |

### 4.3 Why Orchestration Saga, Not Choreography

**Choreography (what we are NOT doing):**
Each service publishes events and other services react. No central coordinator. For example: Booking Service publishes `BOOKING_INITIATED`, Payment Service listens and charges, publishes `PAYMENT_DONE`, Booking Service listens and confirms, publishes `BOOKING_CONFIRMED`, Flight Service listens and finalizes.

**Problem with choreography here:**
- Rollback is hard to track. When payment fails and you need to release seats, which service initiates it? Each service only knows its own step.
- Debugging is hard — the flow is implicit, spread across multiple service logs.
- Adding a new step (e.g., loyalty point reservation) requires changing multiple services.

**Orchestration (what we DO):**
The Booking Service knows the full flow. It calls each step explicitly. It knows what to do at every failure point. The `saga_logs` table gives a complete, queryable audit trail. Adding a step means changing only the Booking Service's `saga.service.ts`.

### 4.4 Idempotency Strategy for All Consumers

Every RabbitMQ consumer and BullMQ worker in this service **must check the current booking status before acting.** This is the single most important implementation rule:

```
Rule: "Always read before write in any consumer/worker"

RabbitMQ PAYMENT_SUCCESS consumer:
  booking = DB.findById(bookingId)
  if !booking → log warning, ack message (booking might have been cleaned up)
  if booking.status === CONFIRMED → ack, return  ← already processed
  if booking.status !== PENDING_PAYMENT → ack, log warning, return
  → proceed with confirmation

BullMQ seat-timeout worker:
  booking = DB.findById(bookingId)
  if booking.status !== PENDING_PAYMENT → job.complete() ← already handled
  → proceed with timeout

Why is this needed?
  - Kafka and RabbitMQ guarantee AT-LEAST-ONCE delivery
  - Network failures can cause the same message to be delivered twice
  - Without this check, a booking could be confirmed twice, or confirmed then
    immediately rolled back when a duplicate PAYMENT_SUCCESS arrives
```

---

## 5. Complete REST API Specification

### Standard Response Envelope

Same cluster-wide envelope:
```json
{ "success": true,  "message": "...", "data": {}, "meta": {}, "traceId": "..." }
{ "success": false, "error": { "code": "...", "message": "...", "details": [] }, "traceId": "..." }
```

---

### Endpoint 1: POST /api/v1/bookings

**Auth required:** Yes — any authenticated role

**Request Body:**
```json
{
  "flightId": "abc123-def456-ghi789",
  "seats": 2,
  "passengerDetails": [
    { "name": "John Doe",  "type": "ADULT" },
    { "name": "Jane Doe",  "type": "ADULT" }
  ],
  "contactEmail": "john.doe@example.com",
  "contactPhone": "+91-9876543210"
}
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Booking created successfully. Please complete payment within 15 minutes.",
  "data": {
    "bookingId":    "booking-uuid-001",
    "flightId":     "abc123-def456-ghi789",
    "seats":        2,
    "status":       "PENDING_PAYMENT",
    "totalAmount":  999800,
    "currency":     "INR",
    "heldUntil":    "2026-05-28T10:15:00.000Z",
    "passengerDetails": [
      { "name": "John Doe", "type": "ADULT" },
      { "name": "Jane Doe", "type": "ADULT" }
    ],
    "contactEmail": "john.doe@example.com",
    "createdAt":    "2026-05-28T10:00:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Why `totalAmount` is in the response:**
The client uses this to call `POST /api/v1/payments/initiate { bookingId, amount }`. The amount shown on the Stripe payment page must match what was locked at booking creation.

**Error Responses:**
```
400 VALIDATION_ERROR            → Zod body validation failed
401 UNAUTHORIZED                → No JWT / missing X-User-Id header
404 NOT_FOUND                   → flightId not found in Flight Service
409 INSUFFICIENT_SEATS          → {
                                    "code": "INSUFFICIENT_SEATS",
                                    "message": "Only 1 seat available, you requested 2",
                                    "availableSeats": 1
                                  }
422 FLIGHT_NOT_AVAILABLE        → flight status is not ACTIVE
422 PASSENGER_COUNT_MISMATCH    → passengerDetails.length !== seats
503 SERVICE_UNAVAILABLE         → Flight Service circuit breaker open
```

---

### Endpoint 2: GET /api/v1/bookings/:id

**Auth required:** Yes

**Authorization:**
- `CUSTOMER`: can only fetch bookings where `userId = req.userId`
- `FLIGHT_ADMIN`, `SUPER_ADMIN`: can fetch any booking

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Booking retrieved successfully.",
  "data": {
    "bookingId":        "booking-uuid-001",
    "userId":           "user-uuid",
    "flightId":         "abc123-def456-ghi789",
    "seats":            2,
    "status":           "CONFIRMED",
    "totalAmount":      999800,
    "currency":         "INR",
    "heldUntil":        "2026-05-28T10:15:00.000Z",
    "confirmedAt":      "2026-05-28T10:08:33.000Z",
    "cancelledAt":      null,
    "passengerDetails": [
      { "name": "John Doe", "type": "ADULT" },
      { "name": "Jane Doe", "type": "ADULT" }
    ],
    "contactEmail": "john.doe@example.com",
    "contactPhone": "+91-9876543210",
    "createdAt":    "2026-05-28T10:00:00.000Z",
    "sagaHistory": [
      { "state": "SEAT_HELD",       "createdAt": "2026-05-28T10:00:02.000Z", "metadata": null },
      { "state": "PAYMENT_SUCCESS", "createdAt": "2026-05-28T10:08:30.000Z", "metadata": null },
      { "state": "COMPLETED",       "createdAt": "2026-05-28T10:08:33.000Z", "metadata": null }
    ]
  },
  "traceId": "tr-f47ac10b"
}
```

**Note:** `sagaHistory` is ordered by `createdAt ASC`. Provides full transparency to admins.

**Error Responses:**
```
401 UNAUTHORIZED  → missing auth
403 FORBIDDEN     → CUSTOMER trying to fetch another user's booking
404 NOT_FOUND     → booking does not exist
```

---

### Endpoint 3: GET /api/v1/bookings

**Auth required:** Yes

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `status` | string | all | `PENDING_PAYMENT \| CONFIRMED \| CANCELLED \| TIMED_OUT` |
| `page` | number | 1 | |
| `limit` | number | 10 | Max 50 |
| `userId` | string | — | SUPER_ADMIN only — fetch bookings for a specific user |

**Authorization:**
- Regular users: always filtered to their own `userId` (ignores `userId` param)
- `SUPER_ADMIN`: can pass `userId` param to fetch another user's bookings

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Bookings retrieved successfully.",
  "data": {
    "bookings": [
      {
        "bookingId":    "booking-uuid-001",
        "flightId":     "abc123-def456-ghi789",
        "seats":        2,
        "status":       "CONFIRMED",
        "totalAmount":  999800,
        "currency":     "INR",
        "confirmedAt":  "2026-05-28T10:08:33.000Z",
        "createdAt":    "2026-05-28T10:00:00.000Z"
      }
    ]
  },
  "meta": {
    "page":       1,
    "limit":      10,
    "total":      3,
    "totalPages": 1
  },
  "traceId": "tr-f47ac10b"
}
```

---

### Endpoint 4: DELETE /api/v1/bookings/:id

**Auth required:** Yes — user can only cancel their own booking

**Success Response — 200 OK (for PENDING_PAYMENT):**
```json
{
  "success": true,
  "message": "Booking cancelled successfully. Seats have been released.",
  "data": {
    "bookingId":  "booking-uuid-001",
    "status":     "CANCELLED",
    "cancelledAt": "2026-05-28T10:05:00.000Z"
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
401 UNAUTHORIZED          → missing auth
403 FORBIDDEN             → trying to cancel another user's booking
404 NOT_FOUND             → booking does not exist
400 BOOKING_ALREADY_INACTIVE → status is already CANCELLED or TIMED_OUT
422 CANNOT_CANCEL_CONFIRMED  → {
                                "code": "CANNOT_CANCEL_CONFIRMED",
                                "message": "Confirmed bookings cannot be self-cancelled. Contact support for a refund.",
                                "supportEmail": "support@skyhub.com"
                              }
503 SERVICE_UNAVAILABLE   → Flight Service unreachable (seat release failed after retries)
```

---

### Endpoint 5: GET /internal/bookings/:id

**Auth required:** No JWT — internal network access only (not proxied by Gateway)

**Purpose:** Notification Service calls this when processing email jobs to fetch full booking details for PDF generation.

**Response (200 OK):**
```json
{
  "bookingId":        "booking-uuid-001",
  "userId":           "user-uuid",
  "flightId":         "abc123-def456-ghi789",
  "seats":            2,
  "status":           "CONFIRMED",
  "totalAmount":      999800,
  "currency":         "INR",
  "confirmedAt":      "2026-05-28T10:08:33.000Z",
  "flightDepartureAt":"2026-10-12T06:30:00.000Z",
  "passengerDetails": [
    { "name": "John Doe", "type": "ADULT" },
    { "name": "Jane Doe", "type": "ADULT" }
  ],
  "contactEmail": "john.doe@example.com",
  "contactPhone": "+91-9876543210"
}
```

**Error Responses:**
```
404 NOT_FOUND → booking does not exist
```

---

### Endpoint 6: GET /health

**Auth required:** No

**Healthy Response — 200 OK:**
```json
{
  "status":    "healthy",
  "service":   "booking-service",
  "version":   "1.0.0",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database":  "ok",
    "rabbitmq":  "ok",
    "kafka":     "ok",
    "bullmq":    "ok"
  }
}
```

---

## 6. Zod Validation Schemas

Implement all schemas in `src/routes/schemas/booking.schemas.ts`.

### CreateBookingSchema

| Field | Rule |
|---|---|
| `flightId` | `z.string().uuid('flightId must be a valid UUID')` |
| `seats` | `z.number().int().min(1).max(9)` |
| `passengerDetails` | `z.array(PassengerSchema).min(1).max(9)` |
| `contactEmail` | `z.string().email()` |
| `contactPhone` | optional `z.string().regex(/^\+?[\d\s\-]{10,15}$/)` |

**PassengerSchema (nested):**

| Field | Rule |
|---|---|
| `name` | `z.string().trim().min(2).max(100)` |
| `type` | `z.enum(['ADULT', 'CHILD', 'INFANT'])` |

**Cross-field `.refine()`:**
- `passengerDetails.length === seats` → error: "Number of passengers must match seats count"
- At least one `ADULT` passenger required → error: "At least one adult passenger is required"

### BookingIdParamSchema

| Field | Rule |
|---|---|
| `id` | `z.string().uuid('Booking ID must be a valid UUID')` |

### ListBookingsQuerySchema

| Field | Rule |
|---|---|
| `status` | optional `z.enum(['PENDING_PAYMENT', 'CONFIRMED', 'CANCELLED', 'TIMED_OUT'])` |
| `page` | optional `z.coerce.number().int().min(1).default(1)` |
| `limit` | optional `z.coerce.number().int().min(1).max(50).default(10)` |
| `userId` | optional `z.string().uuid()` — only respected if role is SUPER_ADMIN |

---

## 7. Message Queue Architecture

### 7.1 RabbitMQ — Exchange and Queue Design

```
Exchange: skyhub.booking  (type: direct, durable: true)
  Binding: routing key "booking.initiated" → Queue: booking.initiated
  DLQ:     booking.initiated.dlq  (after 3 nack + requeue cycles)

Exchange: skyhub.payment  (type: direct, durable: true)
  Binding: routing key "payment.result" → Queue: payment.result
  DLQ:     payment.result.dlq
```

**Booking Service as PRODUCER (via Outbox):**
- Publishes `BOOKING_INITIATED` to `skyhub.booking` exchange, routing key `booking.initiated`
- Published via the Outbox Worker (not directly from the HTTP handler)
- Message is persistent (`{ persistent: true }`) — survives RabbitMQ restarts

**Booking Service as CONSUMER:**
- Consumes from `payment.result` queue
- Handles `PAYMENT_SUCCESS` and `PAYMENT_FAILED` event types
- Uses manual acknowledgement (`noAck: false`)
- On processing success: `channel.ack(message)` — message is removed from queue
- On processing error: `channel.nack(message, false, true)` — message is requeued
- After 3 nacks: message moves to `payment.result.dlq` automatically

**RabbitMQ consumer setup on startup:**
```
1. Connect to RabbitMQ (amqplib)
2. Assert exchange: skyhub.payment (direct, durable)
3. Assert queue: payment.result (durable)
4. Assert DLQ: payment.result.dlq (durable)
5. Bind: payment.result → skyhub.payment with key "payment.result"
6. channel.consume('payment.result', handlePaymentResult, { noAck: false })
```

### 7.2 BOOKING_INITIATED — Full Message Payload

```json
{
  "eventId":       "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "eventType":     "BOOKING_INITIATED",
  "eventVersion":  "1.0",
  "source":        "booking-service",
  "correlationId": "req-abc123",
  "timestamp":     "2026-05-28T10:00:00.000Z",
  "payload": {
    "bookingId":   "booking-uuid-001",
    "userId":      "user-uuid",
    "flightId":    "abc123-def456-ghi789",
    "seats":       2,
    "totalAmount": 999800,
    "currency":    "INR",
    "correlationId": "req-abc123"
  }
}
```

The Payment Service consumes this to know the booking amount for creating the Stripe PaymentIntent.

### 7.3 PAYMENT_RESULT — Expected Incoming Message Shapes

```json
// PAYMENT_SUCCESS
{
  "eventType": "PAYMENT_SUCCESS",
  "payload": {
    "bookingId":       "booking-uuid-001",
    "paymentIntentId": "pi_3abc123",
    "amount":          999800,
    "currency":        "INR"
  }
}

// PAYMENT_FAILED
{
  "eventType": "PAYMENT_FAILED",
  "payload": {
    "bookingId":       "booking-uuid-001",
    "paymentIntentId": "pi_3abc123",
    "failureReason":   "insufficient_funds"
  }
}
```

### 7.4 BOOKING_COMPLETED — Kafka Event (for User Service)

**Topic:** `booking-events`
**Message key:** `userId` (ensures all bookings for the same user go to the same Kafka partition — ordering guarantee for loyalty tier calculations)

```json
{
  "eventId":      "...",
  "eventType":    "BOOKING_COMPLETED",
  "eventVersion": "1.0",
  "source":       "booking-service",
  "correlationId":"...",
  "timestamp":    "2026-05-28T10:08:33.000Z",
  "payload": {
    "bookingId": "booking-uuid-001",
    "userId":    "user-uuid",
    "flightId":  "abc123-def456-ghi789",
    "seats":     2
  }
}
```

The User Service consumes this to increment `booking_count` and recalculate loyalty tier.

### 7.5 BullMQ Queue Design

All BullMQ queues use **Redis DB 3** (shared with Notification Service).

```
Queue: seat-timeout-queue
  Producer: Booking Service (after creating booking)
  Worker:   Booking Service (releases seats when fired)
  Job data: { bookingId, flightId, seats }
  Job ID:   bookingId  (deterministic — prevents duplicate timeout jobs)
  Delay:    15 minutes (900,000 ms)
  Retries:  3 (in case the worker crashes mid-processing)

Queue: email-queue
  Producer: Booking Service (adds jobs, does NOT process them)
  Worker:   Notification Service
  Job data: { bookingId, emailType: 'BOOKING_CONFIRMED' | 'BOOKING_EXPIRED' | 'BOOKING_CANCELLED_BY_USER' }
  Note:     Store ONLY bookingId — Notification Service fetches details via internal HTTP
            NEVER store PII (passenger names, emails) in Redis job data

Queue: reminder-queue
  Producer: Booking Service (adds jobs 24h before departure)
  Worker:   Notification Service
  Job data: { bookingId }
  Job ID:   reminder:{bookingId}  (deterministic — prevents duplicate reminders)
  Delay:    flightDepartureAt - 24 hours - Date.now()
  Note:     If delay is negative (departure in < 24h), add with delay = 0 (fire immediately)
```

**Why store only `bookingId` in BullMQ jobs and not the full booking data?**
BullMQ jobs are serialized to Redis. Storing PII (passenger names, emails, phone numbers) in Redis:
1. Violates data minimization (GDPR principle)
2. Redis data persists until TTL expires — PII would live there even after booking deletion
3. If Redis is compromised, attacker gets raw PII

Storing only `bookingId` is safe. Notification Service fetches fresh data via `GET /internal/bookings/:id` at job processing time — always up-to-date, no PII in Redis.

### 7.6 Outbox Worker — Three-Destination Routing

```
Outbox Worker polls every 5 seconds:
  SELECT * FROM outbox_events WHERE status='PENDING' ORDER BY created_at ASC LIMIT 100

For each event:
  SWITCH destination:

  CASE RABBITMQ:
    Publish to RabbitMQ exchange with routing key from eventType
    On success → mark PUBLISHED
    On failure → leave PENDING (retry next poll)

  CASE KAFKA:
    Publish to Kafka topic 'booking-events' with key = payload.userId
    On success → mark PUBLISHED
    On failure → leave PENDING (retry next poll)

  CASE HTTP:
    Parse payload: { flightId, seats, bookingId }
    HTTP PATCH /internal/flights/{flightId}/release-seats with axios-retry
    Increment attempts on each try
    On success → mark PUBLISHED, INSERT saga_logs(ROLLBACK_COMPLETED)
    On failure after MAX_ATTEMPTS (5):
      mark FAILED
      INSERT saga_logs(ROLLBACK_FAILED, metadata: { attempts, lastError })
      BullMQ alert job → notify ops team
```

---

## 8. External HTTP Calls (Flight Service)

The Booking Service makes synchronous HTTP calls to the Flight Service. These are the only synchronous cross-service HTTP calls in the Booking Service.

### 8.1 HTTP Client Configuration (axios + opossum)

**Base config:**
```
baseURL = env.FLIGHT_SERVICE_INTERNAL_URL  (e.g., http://flight-service:3002)
timeout = 5000 ms (fail fast — don't hold up the booking HTTP response)
```

**axios-retry config (for network transients):**
```
retries: 3
retryDelay: exponential (1s, 2s, 4s)
retryCondition: network errors + 503 responses only
NOTE: Do NOT retry 409 (INSUFFICIENT_SEATS) — that is a business error, not a transient
```

**Circuit Breaker (opossum) config:**
```
timeout:                  5000ms   (request taking > 5s = failure)
errorThresholdPercentage: 50%      (open after 50% of recent calls fail)
resetTimeout:             30000ms  (try again after 30s in HALF-OPEN state)
volumeThreshold:          5        (need at least 5 calls before opening)

Fallback for hold-seats:
  Return 503 { code: 'SERVICE_UNAVAILABLE', message: 'Flight service is temporarily unavailable' }

States:
  CLOSED → normal operation
  OPEN   → returns fallback immediately, no HTTP call made
  HALF-OPEN → one test call. If success → CLOSED. If failure → OPEN again.
```

### 8.2 Call 1: GET Price — GET /api/v1/flights/:flightId

**Purpose:** Fetch `basePrice` and verify flight is ACTIVE before attempting seat hold.

**Response fields used:**
- `data.basePrice` — apply loyalty discount → `discountedPricePerSeat`
- `data.status` — must be `ACTIVE`
- `data.departureDate + data.departureTime` — combine to compute `flightDepartureAt` (stored on booking)

**Error handling:**
- `404` → throw `AppError(404, 'NOT_FOUND', 'Flight not found')`
- `status !== ACTIVE` → throw `AppError(422, 'FLIGHT_NOT_AVAILABLE', 'Flight is not available for booking')`
- Network error → Circuit Breaker handles → `AppError(503, 'SERVICE_UNAVAILABLE', ...)`

### 8.3 Call 2: Hold Seats — PATCH /internal/flights/:flightId/hold-seats

**Body:** `{ seats, bookingId }` (use the pre-generated booking UUID as bookingId)

**Response fields used:**
- `remainingSeats` — stored in saga_log metadata for debugging
- `heldUntil` — stored on booking row

**Error handling:**
- `409 INSUFFICIENT_SEATS` → propagate directly to client with available seats count
- `404` → flight was deleted between price-fetch and hold — return 404 to client
- `5xx` / network → Circuit Breaker fallback → 503 to client

**Why pre-generate the booking UUID?**
The `bookingId` is passed to `hold-seats` so Flight Service can store it in the `SEATS_HELD` outbox event payload. If the Booking Service crashes between the hold-seats call and the DB write, the Flight Service has evidence of which booking caused the hold — useful for manual recovery. The same UUID is then used when inserting the booking row.

### 8.4 Call 3: Release Seats — PATCH /internal/flights/:flightId/release-seats

**Used in:** Payment failed rollback (via Outbox Worker), seat timeout worker, user-initiated cancellation.

**Body:** `{ seats, bookingId }`

**Error handling in Outbox Worker (HTTP destination):**
- Retry up to 5 times with exponential backoff (1s, 2s, 4s, 8s, 16s = ~31s total)
- On all failures: mark outbox as FAILED, write `ROLLBACK_FAILED` saga_log, alert ops

**Error handling in BullMQ worker (seat timeout) and user cancellation:**
- Retry up to 3 times
- If all fail: log critical error (seats are stuck as held — needs manual ops intervention)

---

## 9. Layered Architecture & File Map

```
services/booking-service/
│
├── prisma/
│   ├── schema.prisma              ← Booking, SagaLog, OutboxEvent models
│   ├── migrations/                ← Auto-generated by prisma migrate dev
│   └── seed.ts                    ← Create sample bookings for local dev/testing
│
├── src/
│   │
│   ├── config/
│   │   ├── env.ts                 ← Zod-validated env vars — crash-fast on missing config
│   │   ├── database.ts            ← Prisma client singleton
│   │   ├── rabbitmq.ts            ← amqplib connection + channel setup
│   │   ├── kafka.ts               ← KafkaJS producer (booking-events topic)
│   │   ├── bullmq.ts              ← BullMQ Queue instances + Redis connection (DB 3)
│   │   ├── flightClient.ts        ← Axios instance + Circuit Breaker (opossum) for Flight Service
│   │   └── logger.ts              ← Pino with AsyncLocalStorage
│   │
│   ├── repositories/
│   │   ├── booking.repository.ts  ← Prisma booking queries — no business logic
│   │   ├── sagaLog.repository.ts  ← Append-only saga_log inserts + reads
│   │   └── outbox.repository.ts   ← Insert + query + update outbox_events
│   │
│   ├── services/
│   │   ├── booking.service.ts     ← Create, list, getById, cancel — core booking logic
│   │   └── saga.service.ts        ← Saga step execution: holdSeats, confirmBooking,
│   │                                  rollbackBooking, timeoutBooking
│   │
│   ├── controllers/
│   │   ├── booking.controller.ts  ← HTTP handlers for public/auth routes
│   │   └── internal.controller.ts ← GET /internal/bookings/:id for Notification Service
│   │
│   ├── routes/
│   │   ├── booking.routes.ts      ← /api/v1/bookings routes
│   │   ├── internal.routes.ts     ← /internal/bookings routes
│   │   ├── health.routes.ts       ← GET /health
│   │   ├── metrics.routes.ts      ← GET /metrics
│   │   └── schemas/
│   │       └── booking.schemas.ts ← All Zod schemas (Section 6)
│   │
│   ├── middlewares/
│   │   ├── requireAuth.ts         ← Reads X-User-Id + X-User-Role + X-User-Loyalty-Tier
│   │   ├── validate.ts            ← req.body Zod validation
│   │   ├── validateQuery.ts       ← req.query Zod validation
│   │   ├── validateParams.ts      ← req.params Zod validation
│   │   └── errorHandler.ts        ← Global Express error handler
│   │
│   ├── events/
│   │   ├── consumers/
│   │   │   └── payment.consumer.ts   ← RabbitMQ: consumes payment.result queue
│   │   ├── producers/
│   │   │   ├── booking.producer.ts   ← RabbitMQ: publishes BOOKING_INITIATED envelope
│   │   │   └── loyalty.producer.ts   ← Kafka: publishes BOOKING_COMPLETED envelope
│   │   └── outbox.worker.ts          ← Polls outbox → routes to rabbit/kafka/http publisher
│   │
│   ├── workers/
│   │   └── seatTimeout.worker.ts  ← BullMQ worker: processes seat-timeout-queue jobs
│   │
│   ├── types/
│   │   └── express.d.ts           ← Augments req: userId?, userRole?, loyaltyTier?
│   │
│   ├── utils/
│   │   └── response.utils.ts      ← sendSuccess(), sendError()
│   │
│   ├── app.ts                     ← Express: helmet, cors, routes, error handler
│   └── server.ts                  ← Bootstrap: DB, RabbitMQ, Kafka, BullMQ, Outbox Worker
│
├── tests/
│   ├── unit/
│   │   ├── services/
│   │   │   ├── booking.service.test.ts
│   │   │   └── saga.service.test.ts
│   │   └── workers/
│   │       └── seatTimeout.worker.test.ts
│   └── integration/
│       ├── booking.create.test.ts
│       ├── booking.cancel.test.ts
│       ├── payment.consumer.test.ts    ← most critical integration test
│       └── seatTimeout.integration.test.ts
│
├── .env.example
├── package.json
└── tsconfig.json
```

### Layer Rules

```
Routes     → validate (middleware) → controller
Controller → calls booking.service or saga.service (never repositories directly)
Services   → calls repositories + flightClient + bullmq (no req/res objects)
Repository → Prisma queries only — no logic, no calculations

Events     → payment.consumer.ts calls saga.service directly
Workers    → seatTimeout.worker.ts calls saga.service directly
Outbox     → outbox.worker.ts calls producers (rabbit/kafka) + flightClient (HTTP)

saga.service is the central coordination layer — all state transitions go through it
```

### Key Design: Why `saga.service.ts` is Separate from `booking.service.ts`

`booking.service.ts` handles the HTTP-facing operations: validate, create, list, get, cancel. It deals with the request/response lifecycle.

`saga.service.ts` handles all state machine transitions: `holdSeats`, `confirmBooking`, `rollbackBooking`, `timeoutBooking`. It is called from multiple entry points (HTTP handler for create, RabbitMQ consumer for payment results, BullMQ worker for timeouts). Keeping it separate means each caller uses the same business logic — no duplication, no divergence.

### Key Design: `flightClient.ts`

This file wraps the axios instance and the opossum Circuit Breaker. It exposes three typed functions:
```
getFlightPrice(flightId: string): Promise<FlightPriceInfo>
holdSeats(flightId, seats, bookingId): Promise<HoldSeatsResult>
releaseSeats(flightId, seats, bookingId): Promise<ReleaseSeatsResult>
```

The Circuit Breaker wraps only `holdSeats` — the only synchronous call in the booking creation hot path. `releaseSeats` is called from the outbox worker with its own retry logic, so it does not need the Circuit Breaker wrapper.

---

## 10. npm Dependencies

**File: `services/booking-service/package.json`**

```json
{
  "name": "@skyhub/booking-service",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev":           "tsx watch src/server.ts",
    "build":         "tsc --project tsconfig.json",
    "start":         "node dist/server.js",
    "migrate":       "prisma migrate deploy",
    "migrate:dev":   "prisma migrate dev",
    "seed":          "tsx prisma/seed.ts",
    "lint":          "eslint .",
    "test":          "vitest",
    "test:coverage": "vitest run --coverage",
    "typecheck":     "tsc --noEmit"
  },
  "dependencies": {
    "@prisma/client":        "^5.14.0",
    "@skyhub/common-utils":  "*",
    "@skyhub/shared-types":  "*",
    "amqplib":               "^0.10.3",
    "axios":                 "^1.7.2",
    "axios-retry":           "^4.4.0",
    "bullmq":                "^5.8.0",
    "cors":                  "^2.8.5",
    "dotenv":                "^16.4.5",
    "express":               "^5.2.1",
    "helmet":                "^7.1.0",
    "ioredis":               "^5.3.2",
    "kafkajs":               "^2.2.4",
    "opossum":               "^8.1.0",
    "pino":                  "^9.2.0",
    "pino-http":             "^10.2.0",
    "prom-client":           "^15.1.2",
    "uuid":                  "^9.0.1",
    "zod":                   "^3.23.8"
  },
  "devDependencies": {
    "@types/amqplib":        "^0.10.5",
    "@types/cors":           "^2.8.17",
    "@types/express":        "^5.0.6",
    "@types/node":           "^22.0.0",
    "@types/opossum":        "^8.1.5",
    "@types/supertest":      "^6.0.2",
    "@vitest/coverage-v8":   "^1.6.0",
    "pino-pretty":           "^11.0.0",
    "prisma":                "^5.14.0",
    "supertest":             "^6.3.4",
    "tsx":                   "^4.15.7",
    "vitest":                "^1.6.0"
  }
}
```

### Dependency Explanations

| Package | Why |
|---|---|
| `amqplib` | Official RabbitMQ AMQP 0-9-1 client. Manual channel management gives fine-grained control over message ack/nack. The `@types/amqplib` package provides TypeScript types. |
| `axios` + `axios-retry` | HTTP client for Flight Service calls. `axios-retry` adds configurable retry with exponential backoff. Used for both price-fetch and seat operations. |
| `bullmq` | BullMQ job queue backed by Redis DB 3. Used as a producer (add jobs) AND as a worker (process seat-timeout-queue). Notification Service consumes email-queue and reminder-queue. |
| `ioredis` | BullMQ requires ioredis as its Redis client. Same package, configured with DB 3 URL. |
| `kafkajs` | Producer only — publishes `BOOKING_COMPLETED` to `booking-events`. No consumer needed here. |
| `opossum` | Circuit Breaker implementation. Wraps the `holdSeats` HTTP call. Prevents cascade failure if Flight Service goes down during peak booking traffic. |
| No `bcrypt`, `jose` | Booking Service does not handle passwords or JWT signing. It reads trusted headers from Gateway. |

---

## 11. Environment Variables

**File: `services/booking-service/.env.example`**

```bash
# ── Server ────────────────────────────────────────────────────────────
NODE_ENV=development
PORT=3003
SERVICE_NAME=booking-service

# ── Database ──────────────────────────────────────────────────────────
DATABASE_URL=postgresql://skyhub:skyhub_local@localhost:5432/skyhub_booking_db?connection_limit=10&pool_timeout=10

# ── RabbitMQ ──────────────────────────────────────────────────────────
RABBITMQ_URL=amqp://guest:guest@localhost:5672
# Exchange and queue names — must match Payment Service configuration exactly
RABBITMQ_BOOKING_EXCHANGE=skyhub.booking
RABBITMQ_PAYMENT_EXCHANGE=skyhub.payment
RABBITMQ_BOOKING_QUEUE=booking.initiated
RABBITMQ_PAYMENT_QUEUE=payment.result

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=booking-service
KAFKA_TOPIC_BOOKING_EVENTS=booking-events

# ── Redis / BullMQ ────────────────────────────────────────────────────
# DB 3 is shared between Booking Service (producer) and Notification Service (worker)
REDIS_URL=redis://localhost:6379/3

# ── Flight Service ────────────────────────────────────────────────────
# Internal URL — not exposed to internet
FLIGHT_SERVICE_INTERNAL_URL=http://localhost:3002
# Circuit Breaker thresholds
FLIGHT_CB_TIMEOUT_MS=5000
FLIGHT_CB_ERROR_THRESHOLD=50
FLIGHT_CB_RESET_TIMEOUT_MS=30000

# ── Seat Hold ─────────────────────────────────────────────────────────
# Must match Flight Service SEAT_HOLD_DURATION_MINUTES exactly
SEAT_HOLD_DURATION_MINUTES=15

# ── Outbox Worker ─────────────────────────────────────────────────────
OUTBOX_POLL_INTERVAL_MS=5000
# Max HTTP retry attempts for RELEASE_SEATS outbox events
OUTBOX_HTTP_MAX_ATTEMPTS=5

# ── Observability ─────────────────────────────────────────────────────
LOG_LEVEL=info
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

### Env Validation Schema (Zod) — Key Rules

Implement in `src/config/env.ts`. All required — crash on missing values.

| Variable | Rule |
|---|---|
| `NODE_ENV` | enum: `development \| production \| test` |
| `PORT` | string → transform Number, default `3003` |
| `DATABASE_URL` | `z.string().url()` |
| `RABBITMQ_URL` | `z.string()` — amqp:// or amqps:// |
| `RABBITMQ_BOOKING_EXCHANGE` | `z.string()` |
| `RABBITMQ_PAYMENT_EXCHANGE` | `z.string()` |
| `RABBITMQ_BOOKING_QUEUE` | `z.string()` |
| `RABBITMQ_PAYMENT_QUEUE` | `z.string()` |
| `KAFKA_BROKERS` | `z.string()` — comma-separated |
| `KAFKA_CLIENT_ID` | `z.string()` |
| `KAFKA_TOPIC_BOOKING_EVENTS` | `z.string()` |
| `REDIS_URL` | `z.string()` |
| `FLIGHT_SERVICE_INTERNAL_URL` | `z.string().url()` |
| `FLIGHT_CB_TIMEOUT_MS` | string → Number, default `5000` |
| `FLIGHT_CB_ERROR_THRESHOLD` | string → Number, default `50` |
| `FLIGHT_CB_RESET_TIMEOUT_MS` | string → Number, default `30000` |
| `SEAT_HOLD_DURATION_MINUTES` | string → Number, default `15` |
| `OUTBOX_POLL_INTERVAL_MS` | string → Number, default `5000` |
| `OUTBOX_HTTP_MAX_ATTEMPTS` | string → Number, default `5` |
| `LOG_LEVEL` | enum: `error \| warn \| info \| debug`, default `info` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `z.string().url().optional()` |

---

## 12. Step-by-Step Build Plan

### Step 1: Project Setup & Tooling

1. Create `services/booking-service/` directory
2. Create `package.json` (Section 10)
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
5. Create `src/config/env.ts` (Section 11)
6. Copy `.env.example` to `.env`, fill in values

**Validation:** `npm run typecheck` → zero errors. `npm run dev` → crash with Zod errors if any env var missing.

---

### Step 2: Database Migration

1. Ensure PostgreSQL running and `skyhub_booking_db` exists
2. `cd services/booking-service && npx prisma init`
3. Replace `schema.prisma` with Section 3.3 schema
4. Run `npx prisma migrate dev --name init`
5. Verify in Prisma Studio: `bookings`, `saga_logs`, `outbox_events` tables exist with all columns and enums

**Seed file (`prisma/seed.ts`):** Create 2-3 sample CONFIRMED bookings for local dev. Use realistic future flight departure dates so reminder-queue job delays are positive numbers.

**Validation:** Run `npm run seed`. Query `SELECT status, COUNT(*) FROM bookings GROUP BY status;` to confirm data.

---

### Step 3: Utilities & Common Infrastructure

1. Create `src/config/database.ts` — Prisma singleton (same pattern as other services)
2. Create `src/config/logger.ts` — Pino with AsyncLocalStorage
3. Create `src/utils/response.utils.ts` — sendSuccess, sendError
4. Create `src/types/express.d.ts` — augment Request with `userId?`, `userRole?`, `loyaltyTier?`
5. Create `src/middlewares/requireAuth.ts` — reads `X-User-Id`, `X-User-Role`, `X-User-Loyalty-Tier` headers
6. Create `src/middlewares/validate.ts`, `validateQuery.ts`, `validateParams.ts`
7. Create `src/middlewares/errorHandler.ts` — global AppError handler
8. Create `src/routes/schemas/booking.schemas.ts` (Section 6)

**`requireAuth.ts` note:** This service also needs `X-User-Loyalty-Tier` (to calculate discount on booking creation). Attach it to `req.loyaltyTier`. The discount multiplier logic lives in `booking.service.ts`:
```
SILVER  → 0.95, GOLD → 0.90, PLATINUM → 0.85, default → 0.95
totalAmount = Math.round(basePrice * multiplier) * seats
```

**Validation:** `npm run typecheck` → zero errors.

---

### Step 4: Repository Layer

**`src/repositories/booking.repository.ts`** — key methods:
```
create(data, tx): Promise<Booking>         ← accepts Prisma transaction
findById(id): Promise<Booking | null>
findByIdWithSagaLogs(id): Promise<BookingWithLogs | null>
findByUserId(userId, filters): Promise<{ bookings, total }>
updateStatus(id, status, extras, tx): Promise<Booking>
```

**`src/repositories/sagaLog.repository.ts`** — key methods:
```
create(bookingId, state, metadata, tx): Promise<SagaLog>
findByBookingId(bookingId): Promise<SagaLog[]>
```

**`src/repositories/outbox.repository.ts`** — key methods:
```
create(eventType, destination, payload, tx): Promise<void>
getPending(limit): Promise<OutboxEvent[]>
markPublished(id): Promise<void>
markFailed(id): Promise<void>
incrementAttempts(id): Promise<number>  ← returns new attempt count
```

**Critical: transaction parameter pattern**

All write operations that participate in the saga's atomic transaction accept a Prisma transaction object (`tx`) as a parameter:
```
booking.repository.create(data, tx)
sagaLog.repository.create(bookingId, state, metadata, tx)
outbox.repository.create(type, destination, payload, tx)
```

This allows `saga.service.ts` to call all three inside a single `prisma.$transaction()`:
```
await prisma.$transaction(async (tx) => {
  await bookingRepo.create(bookingData, tx)
  await sagaLogRepo.create(bookingId, 'SEAT_HELD', null, tx)
  await outboxRepo.create('BOOKING_INITIATED', 'RABBITMQ', payload, tx)
})
```

**Validation:** Quick test: call `bookingRepo.create(...)` and verify row in Prisma Studio.

---

### Step 5: Flight Service HTTP Client

1. Create `src/config/flightClient.ts`:

**Functions to implement:**
- `getFlightInfo(flightId)`: GET `/api/v1/flights/:id` — returns `{ basePrice, status, departureDate, departureTime, arrivalDate, arrivalTime }`
- `holdSeats(flightId, seats, bookingId)`: PATCH `/internal/flights/:id/hold-seats` — wrapped in Circuit Breaker
- `releaseSeats(flightId, seats, bookingId)`: PATCH `/internal/flights/:id/release-seats` — uses axios-retry, NO Circuit Breaker

**Circuit Breaker setup:**
```
const flightBreaker = new CircuitBreaker(rawHoldSeatsCall, {
  timeout:                  env.FLIGHT_CB_TIMEOUT_MS,
  errorThresholdPercentage: env.FLIGHT_CB_ERROR_THRESHOLD,
  resetTimeout:             env.FLIGHT_CB_RESET_TIMEOUT_MS,
  volumeThreshold:          5,
})

flightBreaker.fallback(() => {
  throw new AppError(503, 'SERVICE_UNAVAILABLE', 'Flight service is temporarily unavailable. Please try again shortly.')
})

// Log breaker state changes for Prometheus / alerting
flightBreaker.on('open',     () => logger.warn('Flight Service circuit breaker OPENED'))
flightBreaker.on('halfOpen', () => logger.info('Flight Service circuit breaker HALF-OPEN (testing)'))
flightBreaker.on('close',    () => logger.info('Flight Service circuit breaker CLOSED (recovered)'))
```

**`flightDepartureAt` computation:**
Combine `departureDate` (YYYY-MM-DD) and `departureTime` (HH:MM) from the GET response into a full UTC datetime:
```
const flightDepartureAt = new Date(`${departureDate}T${departureTime}:00.000Z`)
```

**Validation:** Start the service + Flight Service. Call `flightClient.getFlightInfo(validId)`. Should return price data. Simulate Flight Service being down → Circuit Breaker should throw 503 after threshold.

---

### Step 6: RabbitMQ + Kafka + BullMQ Setup

1. Create `src/config/rabbitmq.ts`:
   - `connectRabbitMQ()`: creates connection + channel
   - `setupExchangesAndQueues()`: assert exchanges, queues, DLQs, bindings
   - `getChannel()`: returns the channel singleton
   - `checkRabbitMQConnection()`: for health check
   - Handle reconnect on connection error (amqplib does not auto-reconnect — implement a retry loop)

2. Create `src/config/kafka.ts`:
   - KafkaJS producer for `booking-events` topic
   - `allowAutoTopicCreation: false`
   - `connectKafkaProducer()` and `disconnectKafkaProducer()`

3. Create `src/config/bullmq.ts`:
   - ioredis client connected to DB 3
   - Export named Queue instances: `seatTimeoutQueue`, `emailQueue`, `reminderQueue`
   - `checkBullMQConnection()`: `await redis.ping()` for health check

**RabbitMQ exchange/queue topology to assert on startup:**
```
Assert exchange: skyhub.booking  (direct, durable)
Assert exchange: skyhub.payment  (direct, durable)
Assert queue:    booking.initiated     (durable, with deadLetterExchange config)
Assert queue:    booking.initiated.dlq (durable)
Assert queue:    payment.result        (durable, with deadLetterExchange config)
Assert queue:    payment.result.dlq    (durable)
Bind:  booking.initiated → skyhub.booking  key: "booking.initiated"
Bind:  payment.result    → skyhub.payment  key: "payment.result"
```

**Validation:** Start RabbitMQ. Run `connectRabbitMQ()` and `setupExchangesAndQueues()`. Open RabbitMQ Management UI (http://localhost:15672) → Queues tab → verify 4 queues exist.

---

### Step 7: Service Layer + Saga Logic

**`src/services/booking.service.ts`** — key methods:
```
create(userId, loyaltyTier, body): Promise<BookingCreatedResult>
  → price fetch → holdSeats → $transaction(create booking + saga_log + outbox) → schedule BullMQ job

getById(bookingId, requestingUserId, requestingRole): Promise<BookingWithLogs>
  → ownership check → fetch with saga logs

list(userId, role, query): Promise<PaginatedBookings>

cancel(bookingId, requestingUserId, requestingRole): Promise<CancellationResult>
  → ownership check → status check → cancel + release seats
```

**`src/services/saga.service.ts`** — key methods:
```
onPaymentSuccess(bookingId, paymentIntentId): Promise<void>
  → idempotency check → $transaction(confirm + COMPLETED log + BOOKING_COMPLETED outbox)
     → remove BullMQ timeout job → add email + reminder jobs

onPaymentFailed(bookingId, failureReason): Promise<void>
  → idempotency check → $transaction(cancel + ROLLBACK_INITIATED log + RELEASE_SEATS outbox)

onSeatTimeout(bookingId, flightId, seats): Promise<void>
  → idempotency check → $transaction(timeout + TIMED_OUT log)
     → HTTP release seats → add expired email job
```

**Reminder job delay calculation:**
```typescript
function calculateReminderDelay(flightDepartureAt: Date): number {
  const reminderAt = new Date(flightDepartureAt.getTime() - 24 * 60 * 60 * 1000)
  const delay      = reminderAt.getTime() - Date.now()
  return Math.max(delay, 0)  // never negative delay
}
```

**Validation:** Create a booking via POST. Verify: booking row in DB with PENDING_PAYMENT, saga_log row with SEAT_HELD, outbox row with PENDING/RABBITMQ. Verify BullMQ job in Redis (use BullMQ Board or `redis-cli`).

---

### Step 8: Controllers + Routes

**`src/controllers/booking.controller.ts`:**
```
create(req, res)    → booking.service.create() → 201
getById(req, res)   → booking.service.getById() → 200
list(req, res)      → booking.service.list() → 200
cancel(req, res)    → booking.service.cancel() → 200
```

**`src/controllers/internal.controller.ts`:**
```
getBookingDetail(req, res)  → booking.repository.findById() → 200 (flat, no sagaLogs)
```

**`src/routes/booking.routes.ts`:**
```
POST   /          → requireAuth → validate(CreateBookingSchema) → create
GET    /          → requireAuth → validateQuery(ListBookingsQuerySchema) → list
GET    /:id       → requireAuth → validateParams(BookingIdParamSchema) → getById
DELETE /:id       → requireAuth → validateParams(BookingIdParamSchema) → cancel
```

**`src/routes/internal.routes.ts`:**
```
GET    /:id       → (no auth) → validateParams → getBookingDetail
```

**`src/app.ts` route mounting:**
```typescript
app.use('/api/v1/bookings', bookingRouter)
app.use('/internal/bookings', internalRouter)
app.use('/', healthRouter)
app.use('/', metricsRouter)
app.use(globalErrorHandler)
```

**Validation:** Full CRUD via Postman. Create booking → verify 201. GET booking → verify saga history. DELETE pending booking → verify CANCELLED status + seats released in Flight Service DB.

---

### Step 9: Event Consumers + Workers + Outbox

1. Create `src/events/producers/booking.producer.ts` — serializes outbox event into AMQP message, publishes to exchange
2. Create `src/events/producers/loyalty.producer.ts` — serializes outbox event into Kafka message, publishes to `booking-events`
3. Create `src/events/consumers/payment.consumer.ts`:
   - `startPaymentConsumer()`: calls `channel.consume('payment.result', handler, { noAck: false })`
   - `handler`: parse message → switch on `eventType` → call `saga.service.onPaymentSuccess/onPaymentFailed`
   - On success: `channel.ack(message)`
   - On error: `channel.nack(message, false, true)` (requeue once, then DLQ)

4. Create `src/events/outbox.worker.ts`:
   - Poll every `env.OUTBOX_POLL_INTERVAL_MS`
   - Route: RABBITMQ → `bookingProducer.publish(event)`, KAFKA → `loyaltyProducer.publish(event)`, HTTP → `flightClient.releaseSeats(...)` with retry

5. Create `src/workers/seatTimeout.worker.ts`:
   ```
   new Worker('seat-timeout-queue', async (job) => {
     const { bookingId, flightId, seats } = job.data
     await sagaService.onSeatTimeout(bookingId, flightId, seats)
   }, { connection: redis, concurrency: 5 })
   ```

**End-to-end test:**
- Create booking → wait for outbox worker to publish BOOKING_INITIATED
- Use RabbitMQ Management UI to verify message landed in `booking.initiated` queue
- Manually publish a PAYMENT_SUCCESS message to `payment.result` queue
- Verify booking status changes to CONFIRMED
- Verify BOOKING_COMPLETED appears in Kafka `booking-events` topic

---

### Step 10: Health, Metrics, server.ts

1. Create `src/routes/health.routes.ts` — checks Prisma, RabbitMQ, Kafka, BullMQ Redis
2. Create `src/routes/metrics.routes.ts` — collectDefaultMetrics + saga_state_transitions_total, active_pending_bookings gauge, etc.
3. Finalise `src/server.ts` bootstrap:

```
bootstrap():
  1. await prisma.$connect()
  2. await connectRabbitMQ()
  3. await setupExchangesAndQueues()
  4. await kafkaProducer.connect()
  5. startOutboxWorker()
  6. startPaymentConsumer()       ← starts RabbitMQ consumer
  7. startSeatTimeoutWorker()     ← starts BullMQ worker
  8. const server = app.listen(env.PORT)
  9. process.on('SIGTERM', shutdown)
  10. process.on('SIGINT', shutdown)

shutdown():
  1. server.close()
  2. channel.close() + rabbitmqConnection.close()
  3. kafkaProducer.disconnect()
  4. seatTimeoutWorker.close()
  5. redis.quit()
  6. prisma.$disconnect()
  7. process.exit(0)
```

**Full validation:**
```bash
curl http://localhost:3003/health
# Expected: all 4 checks = "ok"

# Create booking
curl -X POST http://localhost:3003/api/v1/bookings \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-uuid" \
  -H "X-User-Role: CUSTOMER" \
  -H "X-User-Loyalty-Tier: GOLD" \
  -d '{"flightId":"<valid-uuid>","seats":1,"passengerDetails":[{"name":"Test User","type":"ADULT"}],"contactEmail":"test@test.com"}'

# After 15 minutes (or reduce SEAT_HOLD_DURATION_MINUTES=1 in .env for testing)
# → booking status should auto-change to TIMED_OUT
# → seats released in Flight Service

# Test graceful shutdown
kill -SIGTERM <pid>
# Logs: "Graceful shutdown complete" — in-flight messages are acked before exit
```

---

## 13. Testing Strategy

### Unit Tests

**`tests/unit/services/saga.service.test.ts`** — most critical unit test:

Mock all dependencies (Prisma, BullMQ, flightClient, producers). Test the pure business logic.

| Test Case | What to Verify |
|---|---|
| `onPaymentSuccess` — normal path | `updateStatus(CONFIRMED)`, COMPLETED saga_log, BOOKING_COMPLETED outbox, timeout job removed |
| `onPaymentSuccess` — already CONFIRMED | No DB writes, no BullMQ ops (idempotency) |
| `onPaymentSuccess` — already CANCELLED | No DB writes (idempotency — late success after cancellation) |
| `onPaymentFailed` — normal path | `updateStatus(CANCELLED)`, ROLLBACK_INITIATED saga_log, RELEASE_SEATS HTTP outbox event |
| `onPaymentFailed` — already CANCELLED | No writes (idempotency) |
| `onSeatTimeout` — still PENDING | `updateStatus(TIMED_OUT)`, release seats HTTP called, expired email job added |
| `onSeatTimeout` — already CONFIRMED | Skip entirely (idempotency) |
| `onSeatTimeout` — already CANCELLED | Skip entirely (idempotency) |

**`tests/unit/services/booking.service.test.ts`:**

| Test Case | What to Verify |
|---|---|
| Create booking — GOLD discount | `totalAmount = Math.round(basePrice * 0.90) * seats` |
| Create booking — SILVER (default) | `totalAmount = Math.round(basePrice * 0.95) * seats` |
| Create booking — Flight Service 409 | Propagates INSUFFICIENT_SEATS AppError |
| Create booking — Flight Service 503 | Propagates SERVICE_UNAVAILABLE AppError |
| Create booking — passenger count mismatch | Caught by Zod before service is called |
| Cancel CONFIRMED booking | Throws `AppError(422, 'CANNOT_CANCEL_CONFIRMED', ...)` |
| Cancel another user's booking | Throws `AppError(403, 'FORBIDDEN', ...)` |
| Cancel PENDING — seat release fails | Returns 503 to client, booking status reverted |
| getById — CUSTOMER fetches own booking | Returns booking + sagaLogs |
| getById — CUSTOMER fetches other's booking | Throws 403 |
| getById — SUPER_ADMIN fetches any booking | Returns booking |

### Integration Tests

**`tests/integration/payment.consumer.test.ts`** — most important integration test:

```
Setup:
  - Real PostgreSQL (test DB or in-memory)
  - Real BullMQ (ioredis-mock or test Redis)
  - Mock flightClient (no real HTTP calls in integration tests)
  - Mock Kafka producer
  - Real RabbitMQ (or use amqplib-mock)

Tests:

  "PAYMENT_SUCCESS confirms booking":
    1. Create a PENDING_PAYMENT booking in DB
    2. Publish PAYMENT_SUCCESS message to payment.result queue
    3. Wait for consumer to process
    4. Assert: booking.status === CONFIRMED, confirmedAt is set
    5. Assert: saga_log has COMPLETED entry
    6. Assert: outbox has BOOKING_COMPLETED (KAFKA) entry

  "PAYMENT_FAILED cancels booking and enqueues seat release":
    1. Create a PENDING_PAYMENT booking
    2. Publish PAYMENT_FAILED message
    3. Assert: booking.status === CANCELLED
    4. Assert: outbox has RELEASE_SEATS (HTTP) entry
    5. Assert: saga_log has ROLLBACK_INITIATED

  "Duplicate PAYMENT_SUCCESS is ignored (idempotency)":
    1. Create already-CONFIRMED booking
    2. Publish PAYMENT_SUCCESS again
    3. Assert: no duplicate DB writes, no duplicate BullMQ jobs

  "PAYMENT_SUCCESS after CANCELLED is ignored":
    1. Create CANCELLED booking (simulates payment arriving late)
    2. Publish PAYMENT_SUCCESS
    3. Assert: booking stays CANCELLED, no COMPLETED log
```

**`tests/integration/booking.create.test.ts`:**
```
✅ Full create flow returns 201 with correct totalAmount (GOLD discount applied)
✅ Booking row + saga_log (SEAT_HELD) + outbox (BOOKING_INITIATED) created atomically
✅ BullMQ job exists for seat-timeout
✅ 409 when Flight Service returns insufficient seats
✅ 503 when Circuit Breaker is open
✅ 422 when passenger count ≠ seats
✅ 401 when no auth headers
```

### Test Coverage Targets

| Layer | Target | Focus |
|---|---|---|
| `saga.service` | 100% | Every state transition + every idempotency case |
| `booking.service` | > 90% | Discount calculation, ownership checks, all cancellation paths |
| `payment.consumer` | 100% | Success, failure, duplicate (idempotency) |
| `seatTimeout.worker` | 100% | Active timeout vs. already-handled idempotency |
| Integration: create | > 85% | Full flow + all error paths |
| Integration: consumer | > 90% | All message types + idempotency |

---

> **This document is the complete build specification for the SkyHub Booking Service.** The Saga Orchestration pattern (Section 4), the idempotency strategy for all consumers (Section 4.4), and the three-destination Outbox Worker (Section 7.6 + Section 9 Step 9) are the most complex parts. Read them carefully before implementation. The most important test to write first is `payment.consumer.test.ts` — it validates that the core saga logic works under all conditions.

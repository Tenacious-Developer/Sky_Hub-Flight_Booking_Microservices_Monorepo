# SkyHub — Search Service: Complete Production-Grade Build Guide

## Table of Contents

1. [Bounded Context & Responsibility](#1-bounded-context--responsibility)
2. [Complete Feature List](#2-complete-feature-list)
3. [MongoDB Schema Design](#3-mongodb-schema-design)
4. [Redis Cache Strategy](#4-redis-cache-strategy)
5. [Complete REST API Specification](#5-complete-rest-api-specification)
6. [Zod Validation Schemas](#6-zod-validation-schemas)
7. [Kafka Event Processing](#7-kafka-event-processing)
8. [Layered Architecture & File Map](#8-layered-architecture--file-map)
9. [npm Dependencies](#9-npm-dependencies)
10. [Environment Variables](#10-environment-variables)
11. [Step-by-Step Build Plan](#11-step-by-step-build-plan)
12. [Testing Strategy](#12-testing-strategy)

---

## 1. Bounded Context & Responsibility

The Search Service is the **CQRS read model** for the entire SkyHub cluster. It never writes to the Flight Service's database — it builds and maintains its own read-optimized MongoDB copy of the flight catalog, updated exclusively via Kafka events published by the Flight Service.

```
WRITE PATH (Flight Service → Kafka → Search Service)

FLIGHT SERVICE
  │
  └── INSERT/UPDATE flight data
      INSERT outbox_events (FLIGHT_UPDATED)
  │
  └── [Background Outbox Worker]
      Publish to Kafka: flight-inventory-events
                │
                ▼
       SEARCH SERVICE (Kafka Consumer)
                │
       1. Upsert flight document into MongoDB
       2. Invalidate stale Redis cache entries
          via tag-based lookup (O(1) not O(N))

─────────────────────────────────────────────

READ PATH (Client → API Gateway → Search Service)

CLIENT
  └── GET /api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY
                │
       API GATEWAY (Port 3000)
                │
  1. Verify RS256 JWT (optional — search works unauthenticated)
  2. Extract loyaltyTier from JWT claims → default SILVER if no JWT
  3. Inject X-User-Loyalty-Tier header
  4. Inject X-Correlation-ID
  5. Proxy to SEARCH SERVICE
                │
                ▼
       SEARCH SERVICE (Port 3006)
                │
  6. Validate query params (Zod)
  7. Build cache key: "search:DEL:BOM:2026-10-12:2:ECONOMY"
  8. Redis GET → HIT: deserialize cached flights
               → MISS: query MongoDB + cache result
  9. Apply loyalty discount in memory:
     SILVER: 5% | GOLD: 10% | PLATINUM: 15%
 10. Apply optional filters (maxPrice, airline, directOnly) in memory
 11. Sort by requested criteria (price | duration | departure)
 12. Paginate
 13. Return result set
```

**Hard boundaries — what this service owns and what it does not touch:**

| Owns | Does NOT own |
|---|---|
| `skyhub_search_db` (MongoDB, exclusive) | `skyhub_flight_db` (PostgreSQL — Flight Service owns it) |
| Read-optimized flight documents | Seat inventory write operations |
| Redis cache DB 1 (search cache) | Redis DB 0 (Gateway + User Service blacklist) |
| Loyalty discount calculation | Loyalty tier assignment (User Service owns it) |
| Tag-based cache invalidation | Flight creation / modification |
| Kafka consumer for flight + user events | Any write to any other service's database |

**Why MongoDB instead of PostgreSQL for the read model?**

The primary search query is a multi-field filter on `origin`, `destination`, `departureDate`, and `cabinClass`. MongoDB's compound B-Tree index on these four fields makes this query sub-millisecond at scale. PostgreSQL could do this too, but MongoDB's flexible document structure allows the Flight Service to add fields to the Kafka event (baggage info, amenities, aircraft type) without requiring a schema migration in the Search Service — the document just grows. This is the CQRS advantage: the read model evolves independently of the write model.

---

## 2. Complete Feature List

### Feature 1: Flight Search (Cache-Aside with Redis)

**Flow:**
1. Client sends `GET /api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY`
2. Zod validates all query params — rejects malformed IATA codes, invalid dates, out-of-range passengers
3. Build canonical cache key: `search:{origin}:{destination}:{date}:{passengers}:{cabin}` (all uppercase)
4. Check Redis DB 1 for this key
5. **Cache HIT (< 150ms SLO):**
   - Deserialize JSON from Redis
   - Apply loyalty discount from `X-User-Loyalty-Tier` header in memory
   - Apply in-memory filters (maxPrice, airline, directOnly)
   - Sort in memory (price | duration | departure)
   - Paginate in memory
   - Return result
6. **Cache MISS (< 800ms SLO):**
   - Query MongoDB with compound-indexed filter
   - Apply `availableSeats >= passengers` filter at DB level
   - Store raw flight list in Redis: `SET key <json> EX 300` (5-minute TTL)
   - Tag the entry for later invalidation: `SADD "tag:flight:{flightId}" cacheKey` for each flight in results
   - Apply discount, filters, sort, paginate in memory
   - Return result

**Why cache raw results (not discounted)?**
Caching the neutral flight list means one cache entry serves all loyalty tiers. If we cached discounted results, SILVER, GOLD, and PLATINUM users would each need a separate cache entry for the same search — tripling Redis memory usage with no benefit.

---

### Feature 2: Single Flight Detail

**Flow:**
1. Client sends `GET /api/v1/search/flights/:flightId`
2. Look up single flight document in MongoDB by `flightId` field
3. Apply loyalty discount from `X-User-Loyalty-Tier` header
4. Return full flight detail (used by the booking confirmation page to display trip summary)

**Why not just call the Flight Service (PostgreSQL write model)?**
The Search Service's MongoDB read model already has all display fields needed — airline, schedule, pricing, amenities. Calling Flight Service adds network latency and couples the read path to the write service. The read model is the right data source for display.

---

### Feature 3: Tag-Based Cache Invalidation

**Triggered by Kafka consumer — not an HTTP endpoint:**

When a `FLIGHT_UPDATED`, `SEATS_HELD`, or `SEATS_RELEASED` event arrives from Kafka:
1. Upsert the flight document in MongoDB
2. Look up all cache keys that reference this flight: `SMEMBERS "tag:flight:{flightId}"`
3. Delete all affected cache keys in one batch: `DEL key1 key2 key3 ...`
4. Delete the tag set itself: `DEL "tag:flight:{flightId}"`

**Why tag-based and not `KEYS *` or `SCAN`?**
`KEYS search:*` is a blocking O(N) command that scans all keys. On a production Redis with millions of keys, this blocks the server for seconds — unacceptable. Tag sets give O(1) tag lookup and O(M) deletion where M is only the affected keys. A flight that appears in 50 cached search results causes exactly 51 Redis operations (1 SMEMBERS + 50 DEL) regardless of total key count.

---

### Feature 4: Loyalty Discount Application

**Applied in-memory per request — never stored in cache:**

```
Discount calculation:
  basePrice (in paise) × discount multiplier = discountedPrice

SILVER  → 0.95 multiplier (5%  off)
GOLD    → 0.90 multiplier (10% off)
PLATINUM → 0.85 multiplier (15% off)

Example: basePrice = 499900 paise (₹4,999)
  SILVER:   499900 × 0.95 = 474905 paise (₹4,749.05)
  GOLD:     499900 × 0.90 = 449910 paise (₹4,499.10)
  PLATINUM: 499900 × 0.85 = 424915 paise (₹4,249.15)
```

**All prices are in paise (minor units) — never floats.**
Integer arithmetic on minor units eliminates floating-point rounding errors. `Math.round(basePrice * 0.95)` is safe because we round once after the single multiplication.

**The loyalty tier source:**
Primary: `X-User-Loyalty-Tier` header injected by the API Gateway from the verified JWT.
Fallback: If header is absent or invalid, default to `SILVER` (unauthenticated users get the base discount).

---

### Feature 5: Kafka Consumer — Flight Inventory Events

**Topic: `flight-inventory-events`** (produced by Flight Service)

Events consumed:

| Event Type | Action |
|---|---|
| `FLIGHT_UPDATED` | Upsert full flight document in MongoDB. Invalidate all cache entries tagged with this `flightId`. |
| `SEATS_HELD` | Update `availableSeats` field in MongoDB (decrement). Invalidate cache entries for this flight. |
| `SEATS_RELEASED` | Update `availableSeats` field in MongoDB (increment). Invalidate cache entries for this flight. |

**Consumer idempotency:** The upsert (`findOneAndUpdate` with `upsert: true`) is naturally idempotent — replaying the same event produces the same document. The cache invalidation is also idempotent — deleting already-deleted keys is a no-op in Redis.

---

### Feature 6: Kafka Consumer — User Identity Events

**Topic: `user-identity-events`** (produced by User Service)

Events consumed:

| Event Type | Action |
|---|---|
| `USER_LOYALTY_UPDATED` | Update the `userTiers` MongoDB collection for this userId. Used for analytics and future personalized ranking. |
| `USER_REGISTERED` | Insert a baseline entry into `userTiers` with `loyaltyTier: 'SILVER'`. |

**Why maintain a `userTiers` collection?**
The primary discount source is the `X-User-Loyalty-Tier` JWT header. The `userTiers` collection is a secondary store enabling future features: personalized flight ranking (prefer airlines a user has booked before), loyalty tier analytics, and a fallback when JWT headers are unavailable. It has no impact on the main search path.

---

### Feature 7: Health Check

**`GET /health` — public, no auth:**

Checks MongoDB connection, Redis connection, and Kafka consumer health. Returns `200` if all pass, `503` if any fail.

---

### Feature 8: Metrics Endpoint

**`GET /metrics` — Prometheus scrape format:**

Standard service metrics plus Search Service-specific:
- `cache_hit_total` — counter, incremented on every Redis cache hit
- `cache_miss_total` — counter, incremented on every MongoDB fallback
- `search_query_duration_ms` — histogram of full search request duration
- `kafka_events_consumed_total{topic, eventType}` — counter per event type
- `cache_invalidation_total` — counter, incremented per cache invalidation event

---

## 3. MongoDB Schema Design

### 3.1 Collections Overview

The Search Service uses two MongoDB collections:

| Collection | Purpose |
|---|---|
| `flights` | Read-model copy of flight catalog — the main search data source |
| `user_tiers` | Local cache of user loyalty tiers — updated from Kafka events |

### 3.2 Flights Collection — Entity Design

```
┌─────────────────────────────────────────────────────────────┐
│                        flights                              │
├─────────────────────────────────────────────────────────────┤
│ _id             ObjectId    PK (auto-generated by MongoDB)  │
│ flightId        String      UNIQUE — mirrors Flight Service │
│ airline         String      e.g., "IndiGo", "Air India"    │
│ flightNumber    String      e.g., "6E-204"                 │
│ origin          String      IATA code: "DEL"               │
│ destination     String      IATA code: "BOM"               │
│ departureDate   String      "2026-10-12" (YYYY-MM-DD)      │
│ departureTime   String      "06:30" (HH:MM, local time)    │
│ arrivalDate     String      "2026-10-12" (may differ)      │
│ arrivalTime     String      "09:15"                        │
│ durationMinutes Number      165 (pre-computed for sort)    │
│ cabinClass      String      "ECONOMY" | "BUSINESS" | "FIRST"│
│ basePrice       Number      499900 (paise — minor units)   │
│ availableSeats  Number      142                            │
│ totalSeats      Number      180                            │
│ aircraft        String      "Airbus A320"                  │
│ stops           Number      0 = direct, 1 = one-stop      │
│ amenities       String[]    ["wifi", "meal", "usb"]        │
│ baggageAllowance Object     { cabin: "7kg", checked: "15kg"}│
│ refundable      Boolean     true                           │
│ updatedAt       Date        set on every Kafka upsert      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                       user_tiers                            │
├─────────────────────────────────────────────────────────────┤
│ _id         ObjectId    PK                                  │
│ userId      String      UNIQUE — mirrors User Service UUID  │
│ loyaltyTier String      "SILVER" | "GOLD" | "PLATINUM"     │
│ updatedAt   Date        set on every Kafka upsert           │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 Column-by-Column Justification

#### `flights` collection

| Field | Type | Why This Design |
|---|---|---|
| `flightId` | String (unique index) | Maps back to the Flight Service PostgreSQL UUID. All Kafka events carry this ID. Unique index enables O(1) upsert lookup. |
| `departureDate` | String "YYYY-MM-DD" | Stored as a string, not a Date, so the compound index uses exact equality (`$eq`) — faster than a range query on DateTime. The search query always asks for a specific date, not a date range. |
| `departureTime` | String "HH:MM" | Display field. Also used for sorting by departure time — lexicographic sort on "HH:MM" strings works correctly within a single day. |
| `durationMinutes` | Number | Pre-computed at upsert time from departure and arrival. Avoids recomputing on every search for sort-by-duration. |
| `basePrice` | Number (integer) | Minor units (paise for INR, cents for USD). Never floats. Discount calculation: `Math.round(basePrice * multiplier)`. |
| `availableSeats` | Number | Updated on every `SEATS_HELD` and `SEATS_RELEASED` Kafka event. The search query filters `availableSeats >= passengers`. |
| `stops` | Number | 0 = direct. Used for `directOnly` filter. Stored as a number so future support for multi-stop can express stop count. |
| `updatedAt` | Date | Set on every Kafka-triggered upsert. Useful for debugging staleness: "when was this document last updated from Kafka?" |

#### `user_tiers` collection

| Field | Type | Why |
|---|---|---|
| `userId` | String (unique) | User Service UUID. Unique index for O(1) lookup. |
| `loyaltyTier` | String | Denormalized from User Service via Kafka. Not the source of truth — the JWT header is. This is a secondary store. |

### 3.4 Complete Mongoose Schemas

**File: `src/models/flight.model.ts`**

```typescript
import mongoose, { Schema, Document, Model } from 'mongoose';

export interface IFlight extends Document {
  flightId:        string;
  airline:         string;
  flightNumber:    string;
  origin:          string;
  destination:     string;
  departureDate:   string;
  departureTime:   string;
  arrivalDate:     string;
  arrivalTime:     string;
  durationMinutes: number;
  cabinClass:      'ECONOMY' | 'BUSINESS' | 'FIRST';
  basePrice:       number;
  availableSeats:  number;
  totalSeats:      number;
  aircraft:        string;
  stops:           number;
  amenities:       string[];
  baggageAllowance: {
    cabin:   string;
    checked: string;
  };
  refundable: boolean;
  updatedAt:  Date;
}

const flightSchema = new Schema<IFlight>(
  {
    flightId:        { type: String, required: true, unique: true },
    airline:         { type: String, required: true },
    flightNumber:    { type: String, required: true },
    origin:          { type: String, required: true, uppercase: true },
    destination:     { type: String, required: true, uppercase: true },
    departureDate:   { type: String, required: true },   // "YYYY-MM-DD"
    departureTime:   { type: String, required: true },   // "HH:MM"
    arrivalDate:     { type: String, required: true },
    arrivalTime:     { type: String, required: true },
    durationMinutes: { type: Number, required: true },
    cabinClass:      { type: String, required: true, enum: ['ECONOMY', 'BUSINESS', 'FIRST'] },
    basePrice:       { type: Number, required: true },   // paise
    availableSeats:  { type: Number, required: true, min: 0 },
    totalSeats:      { type: Number, required: true },
    aircraft:        { type: String, default: '' },
    stops:           { type: Number, default: 0, min: 0 },
    amenities:       [{ type: String }],
    baggageAllowance: {
      cabin:   { type: String, default: '7kg' },
      checked: { type: String, default: '15kg' },
    },
    refundable: { type: Boolean, default: false },
  },
  {
    timestamps: true,      // adds createdAt + updatedAt automatically
    collection: 'flights', // explicit collection name (no Mongoose pluralization surprises)
  }
);

// ─── Indexes ──────────────────────────────────────────────────────────────────

// Primary search query: exact match on all 4 dimensions
// query: { origin, destination, departureDate, cabinClass, availableSeats: { $gte: N } }
flightSchema.index(
  { origin: 1, destination: 1, departureDate: 1, cabinClass: 1 },
  { name: 'idx_search_primary' }
);

// Kafka upsert lookup — findOneAndUpdate({ flightId })
// Already covered by the unique index defined in the schema field above.
// Mongoose creates a unique index automatically for { unique: true } fields.

// Price range filter (optional query param: maxPrice)
flightSchema.index({ basePrice: 1 }, { name: 'idx_price' });

// Sort by available seats (e.g., "most available first")
flightSchema.index({ availableSeats: 1 }, { name: 'idx_available_seats' });

export const FlightModel: Model<IFlight> = mongoose.model<IFlight>('Flight', flightSchema);
```

**File: `src/models/userTier.model.ts`**

```typescript
import mongoose, { Schema, Document, Model } from 'mongoose';

export interface IUserTier extends Document {
  userId:      string;
  loyaltyTier: 'SILVER' | 'GOLD' | 'PLATINUM';
  updatedAt:   Date;
}

const userTierSchema = new Schema<IUserTier>(
  {
    userId:      { type: String, required: true, unique: true },
    loyaltyTier: { type: String, required: true, enum: ['SILVER', 'GOLD', 'PLATINUM'], default: 'SILVER' },
  },
  {
    timestamps: true,
    collection: 'user_tiers',
  }
);

export const UserTierModel: Model<IUserTier> = mongoose.model<IUserTier>('UserTier', userTierSchema);
```

### 3.5 Index Summary

| Collection | Index | Type | Purpose |
|---|---|---|---|
| `flights` | `{ flightId: 1 }` | Unique B-Tree | Kafka upsert lookup, single flight detail |
| `flights` | `{ origin, destination, departureDate, cabinClass }` | Compound B-Tree | Primary search query |
| `flights` | `{ basePrice: 1 }` | B-Tree | Optional price-range pre-filter |
| `flights` | `{ availableSeats: 1 }` | B-Tree | Available seats filter |
| `user_tiers` | `{ userId: 1 }` | Unique B-Tree | User tier lookup by userId |

**Why the compound index key order matters:**
MongoDB uses an index for a query only if the leftmost fields of the index are present in the query. The order `{ origin, destination, departureDate, cabinClass }` is chosen because:
1. Every search query specifies all four fields (exact equality)
2. The cardinality increases left to right: origin (30 airports) → destination (30 airports) → date (365 days) → cabin (3 classes)
3. This order maximizes selectivity at each level of the index tree

---

## 4. Redis Cache Strategy

### 4.1 Cache Key Design

```
Key format:   search:{ORIGIN}:{DESTINATION}:{DATE}:{PASSENGERS}:{CABIN}
Example:      search:DEL:BOM:2026-10-12:2:ECONOMY
TTL:          300 seconds (5 minutes)
Redis DB:     1 (Search Service exclusive — never share with Gateway DB 0)
```

**Why include `passengers` in the cache key?**
The MongoDB query filters `availableSeats >= passengers`. A search for 3 passengers could return fewer flights than a search for 1 passenger (some flights might only have 1–2 seats left). Caching without passengers would serve stale data when a user switches from 1 to 3 passengers. Including passengers gives correct, isolated results per passenger count.

**Why not include `sortBy`, `airline`, `maxPrice`, `directOnly`?**
These are applied in memory after reading from cache, not at the MongoDB level. A single cache entry (for DEL→BOM, Oct 12, 2 pax, ECONOMY) serves all sort/filter combinations for that search. This minimizes cache keys and maximizes hit rate.

### 4.2 Cache Tag Design

When caching a search result, every `flightId` in the result is tagged:

```
For each flight in results:
  Redis SADD "tag:flight:{flightId}" "search:DEL:BOM:2026-10-12:2:ECONOMY"

This creates sets like:
  "tag:flight:abc-123-def" = {
    "search:DEL:BOM:2026-10-12:1:ECONOMY",
    "search:DEL:BOM:2026-10-12:2:ECONOMY",
    "search:DEL:BOM:2026-10-12:3:ECONOMY",
    "search:DEL:BOM:2026-10-13:1:ECONOMY",
    ...
  }
```

**Cache invalidation algorithm (triggered by Kafka event):**

```typescript
async function invalidateFlightCache(flightId: string): Promise<void> {
  const tagKey = `tag:flight:${flightId}`;

  // 1. Get all cache keys that reference this flight — O(1) set lookup
  const affectedKeys = await redis.smembers(tagKey);

  if (affectedKeys.length === 0) return;  // nothing cached, early return

  // 2. Delete all affected search cache entries — O(M) where M = affected keys only
  await redis.del(...affectedKeys);

  // 3. Delete the tag set itself
  await redis.del(tagKey);
}
```

**What NOT to do:**
```typescript
// ❌ NEVER do this — blocks Redis for the duration of the full key scan
const keys = await redis.keys('search:*');
await redis.del(...keys);

// ❌ NEVER do this — SCAN is non-blocking but slow and misses keys under load
let cursor = '0';
do {
  [cursor, keys] = await redis.scan(cursor, 'MATCH', 'search:*', 'COUNT', 100);
  if (keys.length) await redis.del(...keys);
} while (cursor !== '0');
```

### 4.3 Cache Read/Write Sequence

```typescript
// In cache.service.ts

async function getOrSet<T>(
  key: string,
  fetcher: () => Promise<T[]>,
  tagFlightIds: (results: T[]) => string[],  // extract flightIds from results
  ttl = 300
): Promise<T[]> {
  // 1. Try cache first
  const cached = await redis.get(key);
  if (cached) {
    cacheHitCounter.inc();                   // Prometheus counter
    return JSON.parse(cached) as T[];
  }

  cacheMissCounter.inc();

  // 2. Fetch from MongoDB
  const results = await fetcher();

  // 3. Cache the results
  await redis.set(key, JSON.stringify(results), 'EX', ttl);

  // 4. Tag every flight in the results for targeted invalidation
  const flightIds = tagFlightIds(results);
  const pipeline = redis.pipeline();
  for (const flightId of flightIds) {
    pipeline.sadd(`tag:flight:${flightId}`, key);
    pipeline.expire(`tag:flight:${flightId}`, ttl + 60);  // tag lives slightly longer than cache
  }
  await pipeline.exec();

  return results;
}
```

**Why `pipeline.exec()`?**
Pipelining sends all Redis commands in a single network round trip. For 20 flights in a result, we send 40 commands (20 SADD + 20 EXPIRE) in one trip instead of 40 individual TCP calls.

---

## 5. Complete REST API Specification

All public endpoints are prefixed with `/api/v1` at the Gateway level. The Search Service internally handles these paths directly.

### Standard Response Envelope

Same as the rest of the cluster:

```typescript
// Success
{
  success: true,
  message: string,
  data: object | null,
  meta?: { page: number, limit: number, total: number, totalPages: number },
  traceId: string
}

// Error
{
  success: false,
  error: {
    code: string,
    message: string,
    details?: Array<{ field: string, message: string }>
  },
  traceId: string
}
```

---

### Endpoint 1: GET /api/v1/search

**Auth required:** No (JWT optional — tier defaults to SILVER if absent)
**Rate limit:** 100 req / 15 min per IP (enforced at Gateway, standard global limit)

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `from` | string | ✅ | — | 3-letter IATA origin code (e.g., `DEL`) |
| `to` | string | ✅ | — | 3-letter IATA destination code (e.g., `BOM`) |
| `date` | string | ✅ | — | Departure date in `YYYY-MM-DD` format |
| `passengers` | number | ❌ | `1` | Number of passengers (1–9) |
| `cabin` | string | ❌ | `ECONOMY` | `ECONOMY` \| `BUSINESS` \| `FIRST` |
| `page` | number | ❌ | `1` | Page number (≥ 1) |
| `limit` | number | ❌ | `20` | Results per page (1–50) |
| `sortBy` | string | ❌ | `price` | `price` \| `duration` \| `departure` |
| `sortOrder` | string | ❌ | `asc` | `asc` \| `desc` |
| `maxPrice` | number | ❌ | — | Maximum price in paise (inclusive) — filters discounted price |
| `airline` | string | ❌ | — | Filter by airline name (case-insensitive exact match) |
| `directOnly` | boolean | ❌ | `false` | If `true`, exclude flights with stops > 0 |

**Example Request:**
```
GET /api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY&sortBy=price&sortOrder=asc
Authorization: Bearer eyJhbGciOiJSUzI1NiJ9...   (optional)
X-Correlation-ID: tr-f47ac10b-58cc-4372           (injected by Gateway)
X-User-Loyalty-Tier: GOLD                          (injected by Gateway from JWT)
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flights retrieved successfully.",
  "data": {
    "flights": [
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
        "discountedPrice": 449910,
        "discountPercent": 10,
        "loyaltyTier":     "GOLD",
        "availableSeats":  142,
        "totalSeats":      180,
        "aircraft":        "Airbus A320",
        "stops":           0,
        "amenities":       ["usb", "snack"],
        "baggageAllowance": {
          "cabin":   "7kg",
          "checked": "15kg"
        },
        "refundable": false
      }
    ],
    "searchParams": {
      "from":       "DEL",
      "to":         "BOM",
      "date":       "2026-10-12",
      "passengers": 2,
      "cabin":      "ECONOMY"
    }
  },
  "meta": {
    "page":       1,
    "limit":      20,
    "total":      45,
    "totalPages": 3,
    "cacheHit":   true
  },
  "traceId": "tr-f47ac10b-58cc-4372"
}
```

**Key response fields:**
- `basePrice` — original price before any discount (paise)
- `discountedPrice` — price after loyalty discount (paise). This is what the user pays.
- `discountPercent` — 5, 10, or 15. Shown in UI as "10% OFF for GOLD members"
- `loyaltyTier` — echoes back the tier used for this calculation
- `cacheHit` — debug field (can be omitted in production, useful in dev)

**Error Responses:**
```
400 VALIDATION_ERROR    → missing required params, invalid date format, invalid IATA code
404 NOT_FOUND           → no flights match the criteria
500 INTERNAL_ERROR      → unexpected server error
503 SERVICE_UNAVAILABLE → MongoDB or Redis unavailable
```

**400 Validation Error Example:**
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Request validation failed",
    "details": [
      { "field": "from", "message": "Origin must be a 3-letter IATA code" },
      { "field": "date", "message": "Date must be in YYYY-MM-DD format" }
    ]
  },
  "traceId": "tr-abc123"
}
```

---

### Endpoint 2: GET /api/v1/search/flights/:flightId

**Auth required:** No (JWT optional — tier defaults to SILVER)

**Path Parameter:**
- `flightId` — the UUID from the Flight Service (mirrors `flightId` field in MongoDB)

**Example Request:**
```
GET /api/v1/search/flights/abc123-def456-ghi789
X-User-Loyalty-Tier: PLATINUM
X-Correlation-ID: tr-f47ac10b
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Flight retrieved successfully.",
  "data": {
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
    "discountedPrice": 424915,
    "discountPercent": 15,
    "loyaltyTier":     "PLATINUM",
    "availableSeats":  142,
    "totalSeats":      180,
    "aircraft":        "Airbus A320",
    "stops":           0,
    "amenities":       ["usb", "snack"],
    "baggageAllowance": {
      "cabin":   "7kg",
      "checked": "15kg"
    },
    "refundable": false
  },
  "traceId": "tr-f47ac10b"
}
```

**Error Responses:**
```
404 NOT_FOUND   → flightId not in MongoDB read model
500 INTERNAL_ERROR
```

---

### Endpoint 3: GET /health

**Auth required:** No

**Healthy Response — 200 OK:**
```json
{
  "status": "healthy",
  "service": "search-service",
  "version": "1.0.0",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "mongodb": "ok",
    "redis":   "ok",
    "kafka":   "ok"
  }
}
```

**Degraded Response — 503 Service Unavailable:**
```json
{
  "status": "degraded",
  "service": "search-service",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "mongodb": "ok",
    "redis":   "error: ECONNREFUSED 127.0.0.1:6379",
    "kafka":   "ok"
  }
}
```

---

### Endpoint 4: GET /metrics

**Auth required:** No (internal access only — not proxied by Gateway)

Returns Prometheus text format. Prometheus scraper polls this every 15 seconds.

```
# HELP cache_hit_total Total number of Redis cache hits
# TYPE cache_hit_total counter
cache_hit_total 1423

# HELP cache_miss_total Total number of MongoDB fallback queries
# TYPE cache_miss_total counter
cache_miss_total 87

# HELP search_query_duration_ms Search endpoint duration in milliseconds
# TYPE search_query_duration_ms histogram
search_query_duration_ms_bucket{le="50"} 412
search_query_duration_ms_bucket{le="150"} 1198
search_query_duration_ms_bucket{le="800"} 1509
search_query_duration_ms_bucket{le="+Inf"} 1510
search_query_duration_ms_sum 183420
search_query_duration_ms_count 1510

# HELP kafka_events_consumed_total Kafka events consumed by type
# TYPE kafka_events_consumed_total counter
kafka_events_consumed_total{topic="flight-inventory-events",eventType="FLIGHT_UPDATED"} 234
kafka_events_consumed_total{topic="user-identity-events",eventType="USER_LOYALTY_UPDATED"} 17
```

---

## 6. Zod Validation Schemas

**File: `src/routes/schemas/search.schemas.ts`**

```typescript
import { z } from 'zod';

// ─── Query param validation ────────────────────────────────────────────────────
// All query params arrive as strings from Express. Use z.coerce to convert types.

export const SearchQuerySchema = z.object({
  from: z
    .string({ required_error: 'Origin airport code is required' })
    .length(3, 'Origin must be a 3-letter IATA code')
    .toUpperCase(),

  to: z
    .string({ required_error: 'Destination airport code is required' })
    .length(3, 'Destination must be a 3-letter IATA code')
    .toUpperCase(),

  date: z
    .string({ required_error: 'Departure date is required' })
    .regex(/^\d{4}-\d{2}-\d{2}$/, 'Date must be in YYYY-MM-DD format')
    .refine((val) => {
      const d = new Date(val);
      return !isNaN(d.getTime()) && val >= new Date().toISOString().slice(0, 10);
    }, 'Date must be a valid future date'),

  passengers: z.coerce
    .number()
    .int('Passengers must be a whole number')
    .min(1, 'At least 1 passenger required')
    .max(9, 'Maximum 9 passengers per search')
    .default(1),

  cabin: z
    .enum(['ECONOMY', 'BUSINESS', 'FIRST'], {
      errorMap: () => ({ message: 'Cabin must be ECONOMY, BUSINESS, or FIRST' }),
    })
    .default('ECONOMY'),

  page: z.coerce
    .number()
    .int()
    .min(1, 'Page must be at least 1')
    .default(1),

  limit: z.coerce
    .number()
    .int()
    .min(1)
    .max(50, 'Limit cannot exceed 50 results per page')
    .default(20),

  sortBy: z
    .enum(['price', 'duration', 'departure'])
    .default('price'),

  sortOrder: z
    .enum(['asc', 'desc'])
    .default('asc'),

  maxPrice: z.coerce
    .number()
    .int()
    .positive('maxPrice must be a positive integer (in paise)')
    .optional(),

  airline: z
    .string()
    .trim()
    .optional(),

  directOnly: z
    .string()
    .optional()
    .transform((v) => v === 'true'),
}).refine(
  (data) => data.from !== data.to,
  { message: 'Origin and destination cannot be the same', path: ['to'] }
);

export const FlightIdParamSchema = z.object({
  flightId: z.string().uuid('Flight ID must be a valid UUID'),
});

export type SearchQueryInput  = z.infer<typeof SearchQuerySchema>;
export type FlightIdParamInput = z.infer<typeof FlightIdParamSchema>;
```

**Notes on the Zod schema:**
- `.toUpperCase()` normalises `del` → `DEL` before validation — user-friendly
- The `.refine()` check on `date` rejects past dates at the API layer before any DB query
- `z.coerce.number()` safely converts query string `"2"` to number `2`
- `directOnly` coercion: query params cannot be typed booleans, so `"true"` → `true`

---

## 7. Kafka Event Processing

### 7.1 Consumer Configuration

**File: `src/events/consumers/consumer.factory.ts`**

```typescript
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { env } from '../../config/env.js';
import { logger } from '../../config/logger.js';

const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers:  env.KAFKA_BROKERS.split(','),
});

// One consumer instance subscribes to BOTH topics.
// Consumer group: search-service-group
// Kafka assigns partitions across all instances in the group —
// if Search Service is scaled to 3 replicas, partitions are split 3 ways.
export const consumer: Consumer = kafka.consumer({
  groupId: env.KAFKA_GROUP_ID,  // 'search-service-group'
});

export async function startKafkaConsumers(): Promise<void> {
  await consumer.connect();

  await consumer.subscribe({
    topics: [env.KAFKA_TOPIC_FLIGHT_EVENTS, env.KAFKA_TOPIC_USER_EVENTS],
    fromBeginning: false,  // process only new messages from the time this consumer group starts
  });

  await consumer.run({
    eachMessage: async (payload: EachMessagePayload) => {
      const { topic, message } = payload;
      const raw = message.value?.toString();
      if (!raw) return;

      try {
        const event = JSON.parse(raw);
        if (topic === env.KAFKA_TOPIC_FLIGHT_EVENTS) {
          await handleFlightEvent(event);
        } else if (topic === env.KAFKA_TOPIC_USER_EVENTS) {
          await handleUserEvent(event);
        }
      } catch (err) {
        logger.error({ err, topic, offset: message.offset }, 'Failed to process Kafka message');
        // Do NOT throw — throwing here causes the consumer to crash.
        // Log and continue. A future improvement: send to DLQ.
      }
    },
  });
}
```

### 7.2 Flight Event Handler

**File: `src/events/consumers/flight.consumer.ts`**

```typescript
import { FlightModel } from '../../models/flight.model.js';
import { cacheService } from '../../services/cache.service.js';
import { logger } from '../../config/logger.js';

interface FlightUpdatedPayload {
  flightId:        string;
  airline:         string;
  flightNumber:    string;
  origin:          string;
  destination:     string;
  departureDate:   string;
  departureTime:   string;
  arrivalDate:     string;
  arrivalTime:     string;
  durationMinutes: number;
  cabinClass:      string;
  basePrice:       number;
  availableSeats:  number;
  totalSeats:      number;
  aircraft?:       string;
  stops?:          number;
  amenities?:      string[];
  baggageAllowance?: { cabin: string; checked: string };
  refundable?:     boolean;
}

interface SeatsChangedPayload {
  flightId:       string;
  remainingSeats: number;
}

export async function handleFlightEvent(event: {
  eventType: string;
  payload:   unknown;
}): Promise<void> {
  const { eventType, payload } = event;

  switch (eventType) {
    case 'FLIGHT_UPDATED': {
      const p = payload as FlightUpdatedPayload;
      // Upsert: create if not exists, update if exists — naturally idempotent
      await FlightModel.findOneAndUpdate(
        { flightId: p.flightId },
        { $set: { ...p, updatedAt: new Date() } },
        { upsert: true, new: true, runValidators: true }
      );
      await cacheService.invalidateFlightCache(p.flightId);
      logger.info({ flightId: p.flightId }, 'Flight upserted and cache invalidated');
      break;
    }

    case 'SEATS_HELD':
    case 'SEATS_RELEASED': {
      const p = payload as SeatsChangedPayload;
      // Set exact value (not increment/decrement) — idempotent
      // The payload carries the authoritative remaining seat count from Flight Service
      await FlightModel.findOneAndUpdate(
        { flightId: p.flightId },
        { $set: { availableSeats: p.remainingSeats, updatedAt: new Date() } }
      );
      await cacheService.invalidateFlightCache(p.flightId);
      logger.info({ flightId: p.flightId, eventType, remainingSeats: p.remainingSeats }, 'Seat count updated');
      break;
    }

    default:
      logger.warn({ eventType }, 'Unknown flight event type — skipping');
  }
}
```

**Why use `$set` with the full payload instead of `$inc` for seat changes?**
The Flight Service sends `remainingSeats` as the authoritative count after its ACID transaction. Using `$set` means replaying the same event always results in the correct value. Using `$inc` would double-apply on replay — if the consumer processes the event twice (Kafka at-least-once delivery), the seat count would be wrong.

### 7.3 User Event Handler

**File: `src/events/consumers/user.consumer.ts`**

```typescript
import { UserTierModel } from '../../models/userTier.model.js';
import { logger } from '../../config/logger.js';

export async function handleUserEvent(event: {
  eventType: string;
  payload:   unknown;
}): Promise<void> {
  const { eventType, payload } = event;

  switch (eventType) {
    case 'USER_REGISTERED': {
      const p = payload as { userId: string; loyaltyTier: string };
      await UserTierModel.findOneAndUpdate(
        { userId: p.userId },
        { $set: { loyaltyTier: p.loyaltyTier ?? 'SILVER', updatedAt: new Date() } },
        { upsert: true, new: true }
      );
      logger.info({ userId: p.userId }, 'User tier initialized in search read model');
      break;
    }

    case 'USER_LOYALTY_UPDATED': {
      const p = payload as { userId: string; newTier: string };
      await UserTierModel.findOneAndUpdate(
        { userId: p.userId },
        { $set: { loyaltyTier: p.newTier, updatedAt: new Date() } }
      );
      logger.info({ userId: p.userId, newTier: p.newTier }, 'User loyalty tier updated in search read model');
      break;
    }

    default:
      // USER_EMAIL_VERIFIED, USER_LOGGED_IN — not relevant to Search Service
      break;
  }
}
```

---

## 8. Layered Architecture & File Map

```
services/search-service/
│
├── src/
│   │
│   ├── config/
│   │   ├── env.ts              ← Zod-validated env vars — crashes on startup if invalid
│   │   ├── database.ts         ← Mongoose connection singleton
│   │   ├── redis.ts            ← ioredis client singleton (DB 1)
│   │   ├── kafka.ts            ← KafkaJS consumer factory
│   │   └── logger.ts           ← Pino with AsyncLocalStorage correlation injection
│   │
│   ├── models/
│   │   ├── flight.model.ts     ← Mongoose schema + indexes for flights collection
│   │   └── userTier.model.ts   ← Mongoose schema for user_tiers collection
│   │
│   ├── repositories/
│   │   ├── flight.repository.ts   ← All Mongoose flight queries — NO business logic
│   │   └── userTier.repository.ts ← All Mongoose userTier queries
│   │
│   ├── services/
│   │   ├── search.service.ts      ← Orchestrates cache + DB + discount + sort + paginate
│   │   ├── cache.service.ts       ← Redis get/set/tag/invalidate operations
│   │   └── discount.service.ts    ← Loyalty discount calculation (pure functions)
│   │
│   ├── controllers/
│   │   └── search.controller.ts   ← HTTP layer: parse req, call service, send res
│   │
│   ├── routes/
│   │   ├── search.routes.ts       ← GET /api/v1/search and /api/v1/search/flights/:id
│   │   ├── health.routes.ts       ← GET /health — MongoDB + Redis + Kafka liveness
│   │   ├── metrics.routes.ts      ← GET /metrics — Prometheus scrape endpoint
│   │   └── schemas/
│   │       └── search.schemas.ts  ← Zod schemas from Section 6
│   │
│   ├── middlewares/
│   │   ├── validateQuery.ts       ← Zod validation for req.query (GET requests)
│   │   ├── validateParams.ts      ← Zod validation for req.params (path params)
│   │   └── errorHandler.ts        ← Global Express error handler
│   │
│   ├── events/
│   │   └── consumers/
│   │       ├── consumer.factory.ts ← KafkaJS setup, subscription, run loop
│   │       ├── flight.consumer.ts  ← Handles flight-inventory-events
│   │       └── user.consumer.ts    ← Handles user-identity-events
│   │
│   ├── types/
│   │   └── express.d.ts           ← Augments Express Request: req.validatedQuery
│   │
│   ├── utils/
│   │   └── response.utils.ts      ← sendSuccess(), sendError() — same as User Service
│   │
│   ├── app.ts                     ← Express setup: helmet, cors, body-parser, routes
│   └── server.ts                  ← Boot: MongoDB, Redis, Kafka consumers, listen
│
├── tests/
│   ├── unit/
│   │   ├── services/
│   │   │   ├── search.service.test.ts
│   │   │   ├── cache.service.test.ts
│   │   │   └── discount.service.test.ts
│   │   └── consumers/
│   │       └── flight.consumer.test.ts
│   └── integration/
│       ├── search.get.test.ts
│       └── flight.detail.test.ts
│
├── .env.example
├── package.json
└── tsconfig.json
```

### Layer Rules

```
Routes      → validates with middleware → calls Controller
Controller  → calls Services only (no MongoDB, no Redis directly)
Services    → calls Repositories + CacheService (no req/res objects)
Repository  → calls Mongoose only (no business logic, no calculations)

Events      → Kafka consumers call Repositories + CacheService directly
              (consumers are background workers, not in the HTTP path)
```

### Key Implementation Files

**`src/types/express.d.ts`:**
```typescript
import type { SearchQueryInput } from '../routes/schemas/search.schemas.js';

declare namespace Express {
  interface Request {
    validatedQuery?: SearchQueryInput;   // set by validateQuery middleware
    validatedParams?: Record<string, string>; // set by validateParams middleware
  }
}
```

**`src/middlewares/validateQuery.ts`:**
```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodSchema } from 'zod';
import { sendError } from '../utils/response.utils.js';

export function validateQuery(schema: ZodSchema) {
  return (req: Request, res: Response, next: NextFunction) => {
    const result = schema.safeParse(req.query);
    if (!result.success) {
      const details = result.error.errors.map((e) => ({
        field:   e.path.join('.'),
        message: e.message,
      }));
      return sendError({
        res, statusCode: 400, code: 'VALIDATION_ERROR',
        message: 'Invalid query parameters', details,
        traceId: req.headers['x-correlation-id'] as string ?? '',
      });
    }
    req.validatedQuery = result.data as SearchQueryInput;
    next();
  };
}
```

**`src/middlewares/validateParams.ts`:**
```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodSchema } from 'zod';
import { sendError } from '../utils/response.utils.js';

export function validateParams(schema: ZodSchema) {
  return (req: Request, res: Response, next: NextFunction) => {
    const result = schema.safeParse(req.params);
    if (!result.success) {
      const details = result.error.errors.map((e) => ({
        field: e.path.join('.'), message: e.message,
      }));
      return sendError({
        res, statusCode: 400, code: 'VALIDATION_ERROR',
        message: 'Invalid path parameters', details,
        traceId: req.headers['x-correlation-id'] as string ?? '',
      });
    }
    req.validatedParams = result.data;
    next();
  };
}
```

**`src/services/discount.service.ts`:**
```typescript
// Pure functions — no side effects, trivially testable

const DISCOUNT_MULTIPLIERS: Record<string, number> = {
  SILVER:   0.95,
  GOLD:     0.90,
  PLATINUM: 0.85,
};

const DISCOUNT_PERCENT: Record<string, number> = {
  SILVER:   5,
  GOLD:     10,
  PLATINUM: 15,
};

export interface PricedFlight {
  basePrice:       number;
  discountedPrice: number;
  discountPercent: number;
  loyaltyTier:     string;
}

export function applyDiscount(basePrice: number, loyaltyTier: string): PricedFlight {
  const multiplier    = DISCOUNT_MULTIPLIERS[loyaltyTier] ?? DISCOUNT_MULTIPLIERS.SILVER;
  const discountPct   = DISCOUNT_PERCENT[loyaltyTier]     ?? DISCOUNT_PERCENT.SILVER;
  const discountedPrice = Math.round(basePrice * multiplier);  // integer paise, no float

  return { basePrice, discountedPrice, discountPercent: discountPct, loyaltyTier };
}

export function extractLoyaltyTier(header: string | string[] | undefined): string {
  const tier = Array.isArray(header) ? header[0] : header;
  if (tier && ['SILVER', 'GOLD', 'PLATINUM'].includes(tier)) return tier;
  return 'SILVER';  // default for unauthenticated users
}
```

**`src/services/search.service.ts`** — core orchestration:
```typescript
import { flightRepository } from '../repositories/flight.repository.js';
import { cacheService }     from './cache.service.js';
import { applyDiscount }    from './discount.service.js';
import { IFlight }          from '../models/flight.model.js';
import type { SearchQueryInput } from '../routes/schemas/search.schemas.js';

interface FlightResult extends IFlight {
  discountedPrice: number;
  discountPercent: number;
  loyaltyTier:     string;
}

interface SearchResult {
  flights:    FlightResult[];
  total:      number;
  totalPages: number;
  cacheHit:   boolean;
}

export const searchService = {
  async search(params: SearchQueryInput, loyaltyTier: string): Promise<SearchResult> {
    const { from, to, date, passengers, cabin, page, limit, sortBy, sortOrder,
            maxPrice, airline, directOnly } = params;

    const cacheKey = `search:${from}:${to}:${date}:${passengers}:${cabin}`;

    // 1. Attempt cache hit — returns raw flights from MongoDB (no discount applied)
    let rawFlights: IFlight[];
    let cacheHit = false;

    const cached = await cacheService.get<IFlight>(cacheKey);
    if (cached) {
      rawFlights = cached;
      cacheHit = true;
    } else {
      // 2. Cache miss — query MongoDB
      rawFlights = await flightRepository.search({ origin: from, destination: to,
        departureDate: date, cabinClass: cabin, minSeats: passengers });

      // 3. Cache raw results + tag for invalidation
      const flightIds = rawFlights.map((f) => f.flightId);
      await cacheService.set(cacheKey, rawFlights, flightIds);
    }

    // 4. Apply loyalty discount in memory (tier-specific, never cached)
    let results = rawFlights.map((flight) => {
      const priced = applyDiscount(flight.basePrice, loyaltyTier);
      return { ...flight.toObject?.() ?? flight, ...priced } as FlightResult;
    });

    // 5. Apply in-memory filters (user-specific, cannot be cached)
    if (maxPrice !== undefined) {
      results = results.filter((f) => f.discountedPrice <= maxPrice);
    }
    if (airline) {
      results = results.filter((f) =>
        f.airline.toLowerCase() === airline.toLowerCase()
      );
    }
    if (directOnly) {
      results = results.filter((f) => f.stops === 0);
    }

    // 6. Sort in memory
    results.sort((a, b) => {
      let diff: number;
      switch (sortBy) {
        case 'price':     diff = a.discountedPrice - b.discountedPrice; break;
        case 'duration':  diff = a.durationMinutes - b.durationMinutes; break;
        case 'departure': diff = a.departureTime.localeCompare(b.departureTime); break;
        default:          diff = a.discountedPrice - b.discountedPrice;
      }
      return sortOrder === 'asc' ? diff : -diff;
    });

    // 7. Paginate
    const total      = results.length;
    const totalPages = Math.ceil(total / limit);
    const paginated  = results.slice((page - 1) * limit, page * limit);

    return { flights: paginated, total, totalPages, cacheHit };
  },

  async getFlightById(flightId: string, loyaltyTier: string): Promise<FlightResult | null> {
    const flight = await flightRepository.findByFlightId(flightId);
    if (!flight) return null;

    const priced = applyDiscount(flight.basePrice, loyaltyTier);
    return { ...flight.toObject(), ...priced } as FlightResult;
  },
};
```

**`src/repositories/flight.repository.ts`:**
```typescript
import { FlightModel, IFlight } from '../models/flight.model.js';

interface SearchFilter {
  origin:       string;
  destination:  string;
  departureDate: string;
  cabinClass:   string;
  minSeats:     number;
}

export const flightRepository = {
  async search(filter: SearchFilter): Promise<IFlight[]> {
    return FlightModel.find({
      origin:         filter.origin,
      destination:    filter.destination,
      departureDate:  filter.departureDate,
      cabinClass:     filter.cabinClass,
      availableSeats: { $gte: filter.minSeats },
    })
    .hint({ origin: 1, destination: 1, departureDate: 1, cabinClass: 1 })  // force compound index
    .lean()  // returns plain JS objects, not Mongoose Documents — faster for serialization
    .exec();
  },

  async findByFlightId(flightId: string): Promise<IFlight | null> {
    return FlightModel.findOne({ flightId }).lean().exec();
  },

  async upsert(data: Partial<IFlight>): Promise<IFlight> {
    return FlightModel.findOneAndUpdate(
      { flightId: data.flightId },
      { $set: { ...data, updatedAt: new Date() } },
      { upsert: true, new: true, runValidators: true }
    ).exec() as Promise<IFlight>;
  },

  async updateSeats(flightId: string, remainingSeats: number): Promise<void> {
    await FlightModel.findOneAndUpdate(
      { flightId },
      { $set: { availableSeats: remainingSeats, updatedAt: new Date() } }
    ).exec();
  },
};
```

**Why `.lean()`?**
Mongoose Documents are heavy objects with prototype methods, change tracking, and internal state. `.lean()` returns plain JavaScript objects — 3–5× faster deserialization and lower memory. For read-only queries (which is everything in Search Service), `.lean()` is always correct.

**Why `.hint()` on the search query?**
Without `.hint()`, MongoDB's query planner picks the index. Under low traffic this is fine, but under concurrent write load (when seat counts are being updated rapidly), the query planner can make suboptimal choices. Explicitly forcing the compound index guarantees consistent query plans.

**`src/config/database.ts`:**
```typescript
import mongoose from 'mongoose';
import { env }  from './env.js';
import { logger } from './logger.js';

let isConnected = false;

export async function connectDatabase(): Promise<void> {
  if (isConnected) return;

  mongoose.connection.on('connected',    () => logger.info('MongoDB connected'));
  mongoose.connection.on('disconnected', () => { logger.warn('MongoDB disconnected'); isConnected = false; });
  mongoose.connection.on('error',        (err) => logger.error({ err }, 'MongoDB error'));

  await mongoose.connect(env.MONGODB_URI, {
    maxPoolSize:       10,   // max 10 concurrent connections
    minPoolSize:       2,    // keep 2 warm connections ready
    serverSelectionTimeoutMS: 5000,  // fail fast if server not reachable
    socketTimeoutMS:   45000,
  });

  isConnected = true;
}

export async function disconnectDatabase(): Promise<void> {
  await mongoose.disconnect();
  isConnected = false;
}

export async function checkDatabaseConnection(): Promise<'ok' | string> {
  try {
    await mongoose.connection.db?.admin().ping();
    return 'ok';
  } catch (err) {
    return `error: ${(err as Error).message}`;
  }
}
```

**`src/config/redis.ts`:**
```typescript
import Redis  from 'ioredis';
import { env } from './env.js';

export const redis = new Redis(env.REDIS_URL, {
  maxRetriesPerRequest: 3,
  enableReadyCheck:     true,
  lazyConnect:          false,
  db:                   1,  // Search Service uses Redis DB 1 exclusively
});

redis.on('error', (err) => {
  console.error('[Redis] connection error:', err.message);
  // Do not crash — ioredis will auto-reconnect
});

export async function checkRedisConnection(): Promise<'ok' | string> {
  try {
    await redis.ping();
    return 'ok';
  } catch (err) {
    return `error: ${(err as Error).message}`;
  }
}
```

**`src/services/cache.service.ts`:**
```typescript
import { redis } from '../config/redis.js';
import { cacheHitCounter, cacheMissCounter } from '../routes/metrics.routes.js';

export const cacheService = {
  async get<T>(key: string): Promise<T[] | null> {
    const raw = await redis.get(key);
    if (!raw) { cacheMissCounter.inc(); return null; }
    cacheHitCounter.inc();
    return JSON.parse(raw) as T[];
  },

  async set<T>(key: string, data: T[], flightIds: string[], ttl = 300): Promise<void> {
    const pipeline = redis.pipeline();
    pipeline.set(key, JSON.stringify(data), 'EX', ttl);
    for (const flightId of flightIds) {
      pipeline.sadd(`tag:flight:${flightId}`, key);
      pipeline.expire(`tag:flight:${flightId}`, ttl + 60);  // tags live slightly longer
    }
    await pipeline.exec();
  },

  async invalidateFlightCache(flightId: string): Promise<void> {
    const tagKey      = `tag:flight:${flightId}`;
    const affectedKeys = await redis.smembers(tagKey);
    if (affectedKeys.length === 0) return;

    const pipeline = redis.pipeline();
    pipeline.del(...affectedKeys);
    pipeline.del(tagKey);
    await pipeline.exec();
  },
};
```

**`src/routes/health.routes.ts`:**
```typescript
import { Router }               from 'express';
import { checkDatabaseConnection } from '../config/database.js';
import { checkRedisConnection }    from '../config/redis.js';
import { consumer }                from '../events/consumers/consumer.factory.js';

const router = Router();

router.get('/health', async (req, res) => {
  const [mongodb, redisStatus] = await Promise.all([
    checkDatabaseConnection(),
    checkRedisConnection(),
  ]);

  // Kafka consumer health: check if it is still connected
  let kafka: string;
  try {
    // KafkaJS does not expose a ping — check internal state description
    kafka = (consumer as any)._status === 'RUNNING' ? 'ok' : 'error: consumer not running';
  } catch {
    kafka = 'error: status unknown';
  }

  const checks  = { mongodb, redis: redisStatus, kafka };
  const healthy = Object.values(checks).every((v) => v === 'ok');

  res.status(healthy ? 200 : 503).json({
    status:    healthy ? 'healthy' : 'degraded',
    service:   'search-service',
    version:   '1.0.0',
    timestamp: new Date().toISOString(),
    checks,
  });
});

export default router;
```

**`src/routes/metrics.routes.ts`:**
```typescript
import { Router }   from 'express';
import client       from 'prom-client';

const { register, collectDefaultMetrics, Counter, Histogram } = client;

collectDefaultMetrics({ register });  // CPU, memory, event loop lag, GC

export const cacheHitCounter = new Counter({
  name: 'cache_hit_total',
  help: 'Total number of Redis cache hits',
  registers: [register],
});

export const cacheMissCounter = new Counter({
  name: 'cache_miss_total',
  help: 'Total number of MongoDB fallback queries (cache misses)',
  registers: [register],
});

export const searchDurationHistogram = new Histogram({
  name:       'search_query_duration_ms',
  help:       'Duration of search endpoint in milliseconds',
  labelNames: ['cache_result'],
  buckets:    [10, 50, 100, 200, 500, 800, 1500],
  registers:  [register],
});

export const kafkaEventCounter = new Counter({
  name:       'kafka_events_consumed_total',
  help:       'Kafka events consumed by topic and event type',
  labelNames: ['topic', 'eventType'],
  registers:  [register],
});

const router = Router();

router.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

export default router;
```

**`src/app.ts`:**
```typescript
import express        from 'express';
import helmet         from 'helmet';
import cors           from 'cors';
import { pinoHttp }   from 'pino-http';
import { logger }     from './config/logger.js';
import searchRouter   from './routes/search.routes.js';
import healthRouter   from './routes/health.routes.js';
import metricsRouter  from './routes/metrics.routes.js';
import { globalErrorHandler } from './middlewares/errorHandler.js';

export const app = express();

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(pinoHttp({ logger }));

// ─── Routes ───────────────────────────────────────────────────────────────────
app.use('/api/v1',  searchRouter);    // GET /api/v1/search, GET /api/v1/search/flights/:id
app.use('/',        healthRouter);    // GET /health
app.use('/',        metricsRouter);   // GET /metrics

// ─── 404 handler ──────────────────────────────────────────────────────────────
app.use((_req, res) => {
  res.status(404).json({ success: false, error: { code: 'NOT_FOUND', message: 'Route not found' } });
});

// ─── Global error handler ─────────────────────────────────────────────────────
app.use(globalErrorHandler);
```

**`src/server.ts`:**
```typescript
import 'dotenv/config';                        // must be first
import { env }                from './config/env.js';
import { app }                from './app.js';
import { connectDatabase, disconnectDatabase } from './config/database.js';
import { redis }              from './config/redis.js';
import { consumer, startKafkaConsumers } from './events/consumers/consumer.factory.js';
import { logger }             from './config/logger.js';

async function bootstrap(): Promise<void> {
  // 1. Connect all infrastructure before accepting HTTP traffic
  await connectDatabase();
  await redis.ping();                    // verify Redis is reachable
  await startKafkaConsumers();           // connect + subscribe + start run loop

  // 2. Start HTTP server
  const server = app.listen(env.PORT, () => {
    logger.info({ port: env.PORT }, 'Search Service started');
  });

  // 3. Graceful shutdown
  async function shutdown(signal: string): Promise<void> {
    logger.info({ signal }, 'Shutdown signal received — beginning graceful shutdown');

    server.close(() => logger.info('HTTP server closed — no new connections accepted'));

    await consumer.disconnect();          // stop consuming Kafka messages, ack in-flight
    await redis.quit();                   // close Redis connection cleanly
    await disconnectDatabase();           // close all MongoDB connection pool sockets

    logger.info('Graceful shutdown complete');
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

bootstrap().catch((err) => {
  console.error('Fatal bootstrap error:', err);
  process.exit(1);
});
```

---

## 9. npm Dependencies

**File: `services/search-service/package.json`**

```json
{
  "name": "@skyhub/search-service",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev":           "tsx watch src/server.ts",
    "build":         "tsc --project tsconfig.json",
    "start":         "node dist/server.js",
    "lint":          "eslint .",
    "test":          "vitest",
    "test:coverage": "vitest run --coverage",
    "typecheck":     "tsc --noEmit"
  },
  "dependencies": {
    "@skyhub/common-utils": "*",
    "@skyhub/shared-types": "*",
    "cors":         "^2.8.5",
    "dotenv":       "^16.4.5",
    "express":      "^5.2.1",
    "helmet":       "^7.1.0",
    "ioredis":      "^5.3.2",
    "kafkajs":      "^2.2.4",
    "mongoose":     "^8.4.0",
    "pino":         "^9.2.0",
    "pino-http":    "^10.2.0",
    "prom-client":  "^15.1.2",
    "zod":          "^3.23.8"
  },
  "devDependencies": {
    "@types/cors":               "^2.8.17",
    "@types/express":            "^5.0.6",
    "@types/node":               "^22.0.0",
    "@types/supertest":          "^6.0.2",
    "@vitest/coverage-v8":       "^1.6.0",
    "mongodb-memory-server":     "^9.3.0",
    "pino-pretty":               "^11.0.0",
    "supertest":                 "^6.3.4",
    "tsx":                       "^4.15.7",
    "vitest":                    "^1.6.0"
  }
}
```

### Dependency Explanations

| Package | Why |
|---|---|
| `mongoose` | MongoDB ODM with TypeScript support built-in (v8+ includes full types). Schema validation, virtual fields, and the `.lean()` optimization. |
| `ioredis` | Redis client — pipeline support, connection pooling, lazy/eager connect. The `pipeline()` API is essential for batching tag operations. |
| `kafkajs` | Official Kafka Node.js client. Used for the consumer that listens to both `flight-inventory-events` and `user-identity-events`. |
| `prom-client` | Prometheus metrics exporter. Custom counters and histograms for cache hit rate and search latency. |
| `pino` + `pino-http` | Structured JSON logging with automatic request/response timing. `pino-http` wraps every request with timing metadata. |
| `zod` | Query param validation. `z.coerce.*` handles string-to-number conversion from URL query strings. |
| `mongodb-memory-server` | Dev dependency for integration tests. Spins up a real MongoDB process in memory — no Docker required for tests, and tests are isolated from dev data. |
| `@skyhub/common-utils` | Provides `AppError` for typed error handling in the global error handler. |
| `@skyhub/shared-types` | Provides `LoyaltyTier` enum and Kafka event type interfaces (`FlightUpdatedPayload`, etc.). |

---

## 10. Environment Variables

**File: `services/search-service/.env.example`**

```bash
# ── Server ────────────────────────────────────────────────────────────
NODE_ENV=development
PORT=3006
SERVICE_NAME=search-service

# ── MongoDB ───────────────────────────────────────────────────────────
# maxPoolSize=10: keep at most 10 concurrent MongoDB connections
# serverSelectionTimeoutMS and socket timeout are set in code (database.ts)
MONGODB_URI=mongodb://localhost:27017/skyhub_search_db

# ── Redis ─────────────────────────────────────────────────────────────
# DB 1 is exclusively for Search Service cache. DB 0 is Gateway + User Service.
REDIS_URL=redis://localhost:6379/1

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=search-service
KAFKA_GROUP_ID=search-service-group
KAFKA_TOPIC_FLIGHT_EVENTS=flight-inventory-events
KAFKA_TOPIC_USER_EVENTS=user-identity-events

# ── Cache ─────────────────────────────────────────────────────────────
# TTL in seconds for search result cache entries (5 minutes = 300)
# Shorter TTL = fresher data but more MongoDB queries
# Longer TTL = more cache hits but potentially stale seat counts
CACHE_TTL_SECONDS=300

# ── Observability ─────────────────────────────────────────────────────
LOG_LEVEL=info
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

### Env Validation (Startup Crash-Fast)

**File: `src/config/env.ts`**

```typescript
import 'dotenv/config';   // must be first import
import { z } from 'zod';

const envSchema = z.object({
  // ── Server ───────────────────────────────────────────────────────────
  NODE_ENV:      z.enum(['development', 'production', 'test']),
  PORT:          z.string().transform(Number).default('3006'),
  SERVICE_NAME:  z.string().default('search-service'),

  // ── MongoDB ──────────────────────────────────────────────────────────
  MONGODB_URI:   z.string().url('MONGODB_URI must be a valid MongoDB connection string'),

  // ── Redis ────────────────────────────────────────────────────────────
  REDIS_URL:     z.string(),

  // ── Kafka ────────────────────────────────────────────────────────────
  KAFKA_BROKERS:             z.string(),
  KAFKA_CLIENT_ID:           z.string(),
  KAFKA_GROUP_ID:            z.string(),
  KAFKA_TOPIC_FLIGHT_EVENTS: z.string(),
  KAFKA_TOPIC_USER_EVENTS:   z.string(),

  // ── Cache ────────────────────────────────────────────────────────────
  CACHE_TTL_SECONDS: z.string().transform(Number).default('300'),

  // ── Observability ────────────────────────────────────────────────────
  LOG_LEVEL:                   z.enum(['error', 'warn', 'info', 'debug']).default('info'),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url().optional(),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('❌ Invalid environment variables:');
  console.error(parsed.error.flatten().fieldErrors);
  process.exit(1);
}

export const env = parsed.data;
```

---

## 11. Step-by-Step Build Plan

Work through these steps in order. Validate each step before moving to the next.

---

### Step 1: Project Setup & Tooling

**What to do:**
1. Create `services/search-service/` directory if it does not exist
2. Create `services/search-service/package.json` from Section 9 and run `npm install` from the monorepo root
3. Create `services/search-service/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src/**/*"],
  "references": [
    { "path": "../../packages/shared-types" },
    { "path": "../../packages/common-utils" }
  ]
}
```

4. Create `src/config/env.ts` (Section 10 code)
5. Copy `.env.example` to `.env` and fill in values:
   - `MONGODB_URI=mongodb://localhost:27017/skyhub_search_db`
   - `REDIS_URL=redis://localhost:6379/1`
   - `KAFKA_BROKERS=localhost:9092`

**Validation:** Run `npm run typecheck` from `services/search-service/`. Zero errors. Run `npm run dev` — if env vars are missing it should crash with a clear Zod field-by-field error list.

---

### Step 2: MongoDB Models & Connection

**What to do:**
1. Create `src/config/database.ts` (Section 8 code)
2. Create `src/models/flight.model.ts` (Section 3.4 code)
3. Create `src/models/userTier.model.ts` (Section 3.4 code)
4. Verify indexes by connecting and running:

```typescript
// Run once to confirm indexes are created
import { FlightModel } from './src/models/flight.model.js';

const indexes = await FlightModel.collection.indexes();
console.log(JSON.stringify(indexes, null, 2));
// Should show: _id, flightId (unique), idx_search_primary, idx_price, idx_available_seats
```

**Validation:** `npm run dev` — should connect to MongoDB and log "MongoDB connected". Check with MongoDB Compass or `mongosh`: `use skyhub_search_db; db.getCollectionNames()` should return `["flights", "user_tiers"]`.

---

### Step 3: Redis & Cache Service

**What to do:**
1. Create `src/config/redis.ts` (Section 8 code)
2. Create `src/services/cache.service.ts` (Section 8 code)
3. Test cache operations manually:

```typescript
// Quick smoke test — run with tsx
import { redis } from './src/config/redis.js';

await redis.set('test-key', JSON.stringify({ hello: 'world' }), 'EX', 60);
const val = await redis.get('test-key');
console.log(JSON.parse(val!)); // { hello: 'world' }
await redis.del('test-key');
await redis.quit();
```

**Validation:** `redis.ping()` returns `'PONG'`. Set/get/del operations work. Pipeline executes without error.

---

### Step 4: Repository Layer

**What to do:**
1. Create `src/repositories/flight.repository.ts` (Section 8 code)
2. Create `src/repositories/userTier.repository.ts`:

```typescript
import { UserTierModel, IUserTier } from '../models/userTier.model.js';

export const userTierRepository = {
  async upsert(userId: string, loyaltyTier: string): Promise<IUserTier> {
    return UserTierModel.findOneAndUpdate(
      { userId },
      { $set: { loyaltyTier, updatedAt: new Date() } },
      { upsert: true, new: true }
    ).exec() as Promise<IUserTier>;
  },

  async findByUserId(userId: string): Promise<IUserTier | null> {
    return UserTierModel.findOne({ userId }).lean().exec();
  },
};
```

**Validation:** Insert a test flight document via `flightRepository.upsert(...)`, then call `flightRepository.search(...)`. Should return the document. Check in MongoDB Compass.

---

### Step 5: Service Layer

**What to do:**
1. Create `src/services/discount.service.ts` (Section 8 code)
2. Create `src/services/search.service.ts` (Section 8 code)

**Manual validation of discount logic:**
```typescript
import { applyDiscount } from './src/services/discount.service.js';

console.log(applyDiscount(499900, 'SILVER'));
// { basePrice: 499900, discountedPrice: 474905, discountPercent: 5, loyaltyTier: 'SILVER' }

console.log(applyDiscount(499900, 'GOLD'));
// { basePrice: 499900, discountedPrice: 449910, discountPercent: 10, loyaltyTier: 'GOLD' }

console.log(applyDiscount(499900, 'PLATINUM'));
// { basePrice: 499900, discountedPrice: 424915, discountPercent: 15, loyaltyTier: 'PLATINUM' }

// Verify no floating point issues
console.log(applyDiscount(100, 'GOLD')); // { discountedPrice: 90 } — should be exactly 90, not 89.99...
```

**Validation:** `npm run typecheck` — zero errors. All discount calculations return integer paise values.

---

### Step 6: Controller + Routes

**What to do:**
1. Create `src/utils/response.utils.ts` (same as User Service — copy verbatim)
2. Create `src/types/express.d.ts` (Section 8 code)
3. Create `src/middlewares/validateQuery.ts` (Section 8 code)
4. Create `src/middlewares/validateParams.ts` (Section 8 code)
5. Create `src/middlewares/errorHandler.ts`:

```typescript
import { Request, Response, NextFunction } from 'express';
import { AppError } from '@skyhub/common-utils';
import { logger }   from '../config/logger.js';

export function globalErrorHandler(
  err: Error, req: Request, res: Response, _next: NextFunction
) {
  const traceId = req.headers['x-correlation-id'] as string ?? '';

  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      success: false,
      error: { code: err.code, message: err.message },
      traceId,
    });
  }

  logger.error({ err, traceId }, 'Unhandled error');
  res.status(500).json({
    success: false,
    error: { code: 'INTERNAL_ERROR', message: 'An unexpected error occurred' },
    traceId,
  });
}
```

6. Create `src/controllers/search.controller.ts`:

```typescript
import { Request, Response } from 'express';
import { searchService }      from '../services/search.service.js';
import { extractLoyaltyTier } from '../services/discount.service.js';
import { sendSuccess, sendError } from '../utils/response.utils.js';
import type { SearchQueryInput } from '../routes/schemas/search.schemas.js';

export const searchController = {
  async search(req: Request, res: Response): Promise<void> {
    const params      = req.validatedQuery as SearchQueryInput;
    const loyaltyTier = extractLoyaltyTier(req.headers['x-user-loyalty-tier']);
    const traceId     = req.headers['x-correlation-id'] as string ?? '';

    const result = await searchService.search(params, loyaltyTier);

    if (result.flights.length === 0) {
      sendError({
        res, statusCode: 404, code: 'NOT_FOUND',
        message: 'No flights found matching your search criteria',
        traceId,
      });
      return;
    }

    sendSuccess({
      res, statusCode: 200,
      message: 'Flights retrieved successfully.',
      data: {
        flights:      result.flights,
        searchParams: {
          from:       params.from,
          to:         params.to,
          date:       params.date,
          passengers: params.passengers,
          cabin:      params.cabin,
        },
      },
      meta: {
        page:       params.page,
        limit:      params.limit,
        total:      result.total,
        totalPages: result.totalPages,
        cacheHit:   result.cacheHit,
      },
      traceId,
    });
  },

  async getFlightById(req: Request, res: Response): Promise<void> {
    const { flightId } = req.validatedParams as { flightId: string };
    const loyaltyTier  = extractLoyaltyTier(req.headers['x-user-loyalty-tier']);
    const traceId      = req.headers['x-correlation-id'] as string ?? '';

    const flight = await searchService.getFlightById(flightId, loyaltyTier);

    if (!flight) {
      sendError({
        res, statusCode: 404, code: 'NOT_FOUND',
        message: 'Flight not found in search index',
        traceId,
      });
      return;
    }

    sendSuccess({ res, statusCode: 200, message: 'Flight retrieved successfully.', data: flight, traceId });
  },
};
```

7. Create `src/routes/schemas/search.schemas.ts` (Section 6 code)
8. Create `src/routes/search.routes.ts`:

```typescript
import { Router }           from 'express';
import { searchController } from '../controllers/search.controller.js';
import { validateQuery }    from '../middlewares/validateQuery.js';
import { validateParams }   from '../middlewares/validateParams.js';
import { SearchQuerySchema, FlightIdParamSchema } from './schemas/search.schemas.js';

const router = Router();

// GET /api/v1/search?from=DEL&to=BOM&date=...
router.get(
  '/search',
  validateQuery(SearchQuerySchema),
  searchController.search
);

// GET /api/v1/search/flights/:flightId
router.get(
  '/search/flights/:flightId',
  validateParams(FlightIdParamSchema),
  searchController.getFlightById
);

export default router;
```

9. Create `src/config/logger.ts`:

```typescript
import pino from 'pino';
import { AsyncLocalStorage } from 'async_hooks';
import { env } from './env.js';

interface RequestContext {
  correlationId?: string;
  userId?:        string;
}

export const asyncStorage = new AsyncLocalStorage<RequestContext>();

export const logger = pino({
  level: env.LOG_LEVEL,
  base:  { service: env.SERVICE_NAME },
  timestamp: pino.stdTimeFunctions.isoTime,
  mixin() {
    return asyncStorage.getStore() ?? {};
  },
});
```

10. Create `src/app.ts` (Section 8 code)

**Validation:** Start the service. Hit `GET /api/v1/search` without query params — should return 400 with field-level Zod errors. Hit with valid params but no flights in MongoDB — should return 404.

---

### Step 7: Kafka Consumers

**What to do:**
1. Create `src/events/consumers/consumer.factory.ts` (Section 7.1 code)
2. Create `src/events/consumers/flight.consumer.ts` (Section 7.2 code)
3. Create `src/events/consumers/user.consumer.ts` (Section 7.3 code)
4. Wire together in `consumer.factory.ts`:

```typescript
// Add these imports at the top of consumer.factory.ts
import { handleFlightEvent } from './flight.consumer.js';
import { handleUserEvent }   from './user.consumer.js';
```

5. Add Kafka consumer health check to `health.routes.ts`

**Manual end-to-end test:**

Use `kafkajs` admin or any Kafka UI to publish a test `FLIGHT_UPDATED` message to `flight-inventory-events`:

```json
{
  "eventId": "test-001",
  "eventType": "FLIGHT_UPDATED",
  "eventVersion": "1.0",
  "source": "flight-service",
  "correlationId": "test",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "payload": {
    "flightId": "test-flight-uuid-001",
    "airline": "IndiGo",
    "flightNumber": "6E-204",
    "origin": "DEL",
    "destination": "BOM",
    "departureDate": "2026-10-12",
    "departureTime": "06:30",
    "arrivalDate": "2026-10-12",
    "arrivalTime": "09:15",
    "durationMinutes": 165,
    "cabinClass": "ECONOMY",
    "basePrice": 499900,
    "availableSeats": 142,
    "totalSeats": 180,
    "stops": 0
  }
}
```

**Validation:** After publishing the event, check MongoDB Compass — a document should appear in the `flights` collection. Then hit the search endpoint:
```
GET /api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=1&cabin=ECONOMY
```
Should return the test flight with a 5% discount (SILVER, no JWT).

---

### Step 8: Health, Metrics, server.ts

**What to do:**
1. Create `src/routes/health.routes.ts` (Section 8 code)
2. Create `src/routes/metrics.routes.ts` (Section 8 code)
3. Create `src/server.ts` (Section 8 code)
4. Update `src/app.ts` to include health and metrics routers

**Full bootstrap validation:**
```bash
# Start infrastructure
docker compose up -d

# Start search service
npm run dev

# Check health
curl http://localhost:3006/health
# Expected: { "status": "healthy", "checks": { "mongodb": "ok", "redis": "ok", "kafka": "ok" } }

# Check metrics
curl http://localhost:3006/metrics
# Expected: Prometheus text with cache_hit_total, search_query_duration_ms, etc.
```

---

### Step 9: Full End-to-End Validation

**Test the complete data flow — write path then read path:**

```bash
# 1. Seed test flights via Kafka (publish FLIGHT_UPDATED events)
# Use a script or Kafka UI — see Step 7 test payload as template

# 2. Run a search (cache MISS — first request)
curl "http://localhost:3006/api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY"
# Response should include: "cacheHit": false

# 3. Run same search again (cache HIT — within 5 min TTL)
curl "http://localhost:3006/api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY"
# Response should include: "cacheHit": true

# 4. Verify loyalty discounts
curl "http://localhost:3006/api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY" \
  -H "X-User-Loyalty-Tier: PLATINUM"
# discountedPrice should be 15% less than basePrice

# 5. Publish SEATS_HELD event — triggers cache invalidation
# (Kafka message: { eventType: "SEATS_HELD", payload: { flightId: "...", remainingSeats: 140 } })

# 6. Run same search (cache MISS again — old entry was invalidated)
curl "http://localhost:3006/api/v1/search?from=DEL&to=BOM&date=2026-10-12&passengers=2&cabin=ECONOMY"
# Response should include: "cacheHit": false, and availableSeats: 140

# 7. Test graceful shutdown
kill -SIGTERM <pid>
# Logs should show: "Graceful shutdown complete"
```

---

## 12. Testing Strategy

### Unit Tests

**`tests/unit/services/discount.service.test.ts`:**

```typescript
import { describe, it, expect } from 'vitest';
import { applyDiscount, extractLoyaltyTier } from '../../../src/services/discount.service.js';

describe('applyDiscount', () => {
  it('applies 5% discount for SILVER', () => {
    const result = applyDiscount(499900, 'SILVER');
    expect(result.discountedPrice).toBe(474905);
    expect(result.discountPercent).toBe(5);
  });

  it('applies 10% discount for GOLD', () => {
    const result = applyDiscount(499900, 'GOLD');
    expect(result.discountedPrice).toBe(449910);
    expect(result.discountPercent).toBe(10);
  });

  it('applies 15% discount for PLATINUM', () => {
    const result = applyDiscount(499900, 'PLATINUM');
    expect(result.discountedPrice).toBe(424915);
    expect(result.discountPercent).toBe(15);
  });

  it('returns integer paise — no floating point', () => {
    const result = applyDiscount(100, 'GOLD');
    expect(result.discountedPrice).toBe(90);
    expect(Number.isInteger(result.discountedPrice)).toBe(true);
  });

  it('defaults to SILVER for unknown tier', () => {
    const result = applyDiscount(100000, 'BRONZE');
    expect(result.discountPercent).toBe(5);
  });
});

describe('extractLoyaltyTier', () => {
  it('returns tier from valid header', () => {
    expect(extractLoyaltyTier('GOLD')).toBe('GOLD');
  });

  it('defaults to SILVER for missing header', () => {
    expect(extractLoyaltyTier(undefined)).toBe('SILVER');
  });

  it('defaults to SILVER for invalid tier string', () => {
    expect(extractLoyaltyTier('DIAMOND')).toBe('SILVER');
  });
});
```

**`tests/unit/services/cache.service.test.ts`:**

```typescript
// Use ioredis-mock to test cache logic without a real Redis
import { describe, it, expect, beforeEach } from 'vitest';
// Mock the redis module and test cacheService in isolation
```

### Integration Tests

**Setup with `mongodb-memory-server`:**

```typescript
// tests/setup.ts
import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose              from 'mongoose';
import { beforeAll, afterAll } from 'vitest';

let mongod: MongoMemoryServer;

beforeAll(async () => {
  mongod = await MongoMemoryServer.create();
  await mongoose.connect(mongod.getUri());
});

afterAll(async () => {
  await mongoose.disconnect();
  await mongod.stop();
});
```

**`tests/integration/search.get.test.ts`:**

```typescript
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import supertest from 'supertest';
import { app }   from '../../src/app.js';
import { FlightModel } from '../../src/models/flight.model.js';

const request = supertest(app);

const testFlight = {
  flightId:        'test-flight-001',
  airline:         'IndiGo',
  flightNumber:    '6E-204',
  origin:          'DEL',
  destination:     'BOM',
  departureDate:   '2026-10-12',
  departureTime:   '06:30',
  arrivalDate:     '2026-10-12',
  arrivalTime:     '09:15',
  durationMinutes: 165,
  cabinClass:      'ECONOMY',
  basePrice:       499900,
  availableSeats:  142,
  totalSeats:      180,
  stops:           0,
};

beforeEach(async () => {
  await FlightModel.deleteMany({});
  await FlightModel.create(testFlight);
});

describe('GET /api/v1/search', () => {
  it('returns 400 when required params are missing', async () => {
    const res = await request.get('/api/v1/search');
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('VALIDATION_ERROR');
    expect(res.body.error.details).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ field: 'from' }),
        expect.objectContaining({ field: 'to' }),
        expect.objectContaining({ field: 'date' }),
      ])
    );
  });

  it('returns 400 for past date', async () => {
    const res = await request.get('/api/v1/search?from=DEL&to=BOM&date=2020-01-01');
    expect(res.status).toBe(400);
    expect(res.body.error.details[0].field).toBe('date');
  });

  it('returns 400 when origin equals destination', async () => {
    const res = await request.get('/api/v1/search?from=DEL&to=DEL&date=2026-10-12');
    expect(res.status).toBe(400);
  });

  it('returns 404 when no flights match', async () => {
    const res = await request.get('/api/v1/search?from=BLR&to=HYD&date=2026-10-12');
    expect(res.status).toBe(404);
  });

  it('returns flights with SILVER discount by default', async () => {
    const res = await request.get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12');
    expect(res.status).toBe(200);
    expect(res.body.data.flights).toHaveLength(1);
    expect(res.body.data.flights[0].discountedPrice).toBe(474905); // 5% off
    expect(res.body.data.flights[0].discountPercent).toBe(5);
  });

  it('applies GOLD discount from header', async () => {
    const res = await request
      .get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12')
      .set('X-User-Loyalty-Tier', 'GOLD');
    expect(res.body.data.flights[0].discountedPrice).toBe(449910); // 10% off
  });

  it('applies PLATINUM discount from header', async () => {
    const res = await request
      .get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12')
      .set('X-User-Loyalty-Tier', 'PLATINUM');
    expect(res.body.data.flights[0].discountedPrice).toBe(424915); // 15% off
  });

  it('filters by directOnly=true', async () => {
    await FlightModel.create({ ...testFlight, flightId: 'one-stop-flight', stops: 1 });
    const res = await request.get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12&directOnly=true');
    expect(res.body.data.flights).toHaveLength(1);
    expect(res.body.data.flights[0].stops).toBe(0);
  });

  it('filters by maxPrice in paise', async () => {
    await FlightModel.create({ ...testFlight, flightId: 'expensive-flight', basePrice: 1000000 });
    // SILVER discount: 1000000 * 0.95 = 950000 — should be excluded by maxPrice=500000
    const res = await request.get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12&maxPrice=500000');
    expect(res.body.data.flights.every((f: { discountedPrice: number }) => f.discountedPrice <= 500000)).toBe(true);
  });

  it('returns pagination meta', async () => {
    const res = await request.get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12&page=1&limit=1');
    expect(res.body.meta).toMatchObject({ page: 1, limit: 1, total: 1, totalPages: 1 });
  });

  it('sorts by price ascending by default', async () => {
    await FlightModel.create({ ...testFlight, flightId: 'cheap-flight', basePrice: 100000 });
    const res = await request.get('/api/v1/search?from=DEL&to=BOM&date=2026-10-12');
    const prices = res.body.data.flights.map((f: { discountedPrice: number }) => f.discountedPrice);
    expect(prices).toEqual([...prices].sort((a, b) => a - b));
  });
});

describe('GET /api/v1/search/flights/:flightId', () => {
  it('returns flight detail with discount applied', async () => {
    const res = await request
      .get('/api/v1/search/flights/test-flight-001')
      .set('X-User-Loyalty-Tier', 'GOLD');
    expect(res.status).toBe(200);
    expect(res.body.data.flightId).toBe('test-flight-001');
    expect(res.body.data.discountedPrice).toBe(449910);
  });

  it('returns 400 for non-UUID flightId', async () => {
    const res = await request.get('/api/v1/search/flights/not-a-uuid');
    expect(res.status).toBe(400);
  });

  it('returns 404 for unknown flightId', async () => {
    const res = await request.get(`/api/v1/search/flights/00000000-0000-0000-0000-000000000000`);
    expect(res.status).toBe(404);
  });
});
```

### Test Coverage Targets

| Layer | Target | What to Test |
|---|---|---|
| `discount.service` | 100% | All 3 tiers, edge cases (unknown tier, zero price, non-integer result) |
| `search.service` | > 90% | Sort orders, filter combinations, pagination boundaries, empty results |
| `flight.consumer` | > 85% | FLIGHT_UPDATED upsert, SEATS_HELD/RELEASED updates, idempotency (replay same event) |
| Integration: GET /search | > 80% | Happy path, all query params, error cases |
| Integration: GET /health | > 70% | Healthy, degraded |

### Running Tests

```bash
# Unit + integration
npm run test

# Watch mode during development
npm run test -- --watch

# Coverage report
npm run test:coverage
# Target: > 80% line coverage overall

# Run only unit tests
npm run test -- tests/unit

# Run only integration tests
npm run test -- tests/integration
```

---

> **This document is the complete build specification for the SkyHub Search Service.** Every file, every design decision, and every test case needed to build this service from scratch is documented above. Build the steps in order, validate each step before proceeding, and the service will work correctly within the SkyHub cluster.

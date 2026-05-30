# SkyHub — User Service: Complete Production-Grade Build Guide

## Table of Contents

1. [Bounded Context & Responsibility](#1-bounded-context--responsibility)
2. [Complete Feature List](#2-complete-feature-list)
3. [Database Design & Prisma Schema](#3-database-design--prisma-schema)
4. [Security Architecture](#4-security-architecture)
5. [Complete REST API Specification](#5-complete-rest-api-specification)
6. [Zod Validation Schemas](#6-zod-validation-schemas)
7. [Layered Architecture & File Map](#7-layered-architecture--file-map)
8. [npm Dependencies](#8-npm-dependencies)
9. [Environment Variables](#9-environment-variables)
10. [Step-by-Step Build Plan](#10-step-by-step-build-plan)
11. [Testing Strategy](#11-testing-strategy)

---

## 1. Bounded Context & Responsibility

The User Service is the **authoritative identity provider** for the entire SkyHub cluster. Every other service is a consumer of identity — not a producer.

```
[ CLIENT ]
    │
    └── POST /api/v1/auth/login ──► API GATEWAY (Port 3000)
                                         │
                         1. Rate limit check (Redis DB 0)
                         2. No JWT needed — public route
                         3. Generate X-Correlation-ID
                                         │
                                         ▼
                              USER SERVICE (Port 3001)
                                         │
                         4. Zod validate input
                         5. Query PostgreSQL (skyhub_user_db)
                         6. bcrypt.compare
                         7. Sign RS256 JWT (private key)
                         8. Store hashed refresh token
                         9. Write to outbox_events table
                                         │
                                         ▼
                              Return { accessToken, refreshToken }

(Background — Outbox Worker)
    Reads outbox_events → Kafka: user-identity-events
```

**Hard boundaries — what this service owns and what it does not touch:**

| Owns | Does NOT own |
|---|---|
| `skyhub_user_db` (exclusive) | Any other service's database |
| Password hashes | Booking records |
| JWT signing (private key) | Flight inventory |
| Refresh token store | Payment records |
| Loyalty tier tracking | Search index |
| Role assignments | Notification templates |

**Data contract with other services:** Other services receive user data ONLY via:
1. JWT claims (`sub`, `role`, `loyaltyTier`, `jti`) injected as headers by the Gateway
2. Kafka events on the `user-identity-events` topic

No other service calls `SELECT * FROM users` — ever.

---

## 2. Complete Feature List

### Feature 1: User Registration with Email Verification

**Flow:**
1. Client sends `{ name, email, password }` to `POST /api/v1/auth/register`
2. Zod validates: name (min 2 chars), email (valid format), password (min 8 chars, complexity rules)
3. Check if email already exists → 409 Conflict if yes
4. Hash password with `bcrypt(password, 12)` — ~200ms intentionally
5. Generate email verification token: `crypto.randomBytes(32).toString('hex')` (64 hex chars)
6. Store `SHA-256(token)` hash in DB — never the raw token
7. In ONE atomic DB transaction:
   - `INSERT INTO users (...)` with `email_verified = false`, `is_active = true`
   - `INSERT INTO outbox_events (type='USER_REGISTERED', ...)`
8. Send verification email with link: `https://skyhub.com/verify-email?token=<raw_token>`
9. Return `201 Created` — user must verify email before they can log in

**Why email verification?**
Without it, anyone can register with someone else's email address. That person then gets spam or account recovery emails they didn't request. It also prevents throwaway registrations for abusing the system.

**Why store a hash of the verification token (not the raw token)?**
The DB could be leaked via SQL injection or a backup exposure. If the raw token is stored, an attacker reads it and verifies any email. SHA-256 of the token is useless without the original.

---

### Feature 2: Login with Account Lockout Protection

**Flow:**
1. Zod validates `{ email, password }`
2. Find user by email (B-Tree indexed — sub-millisecond)
3. Check `is_active = true` → 401 if false (banned account)
4. Check `email_verified = true` → 403 if false (ask them to verify first)
5. Check `locked_until` — if set and `locked_until > NOW()` → 423 Locked, return how many seconds remain
6. Run `bcrypt.compare(password, passwordHash)` — takes ~200ms
7. If password wrong:
   - Increment `failed_login_attempts`
   - If `failed_login_attempts >= 5`: set `locked_until = NOW() + 30 minutes`, return 423
   - Else return 401
8. If password correct:
   - Reset `failed_login_attempts = 0`
   - Update `last_login_at = NOW()`
   - Sign RS256 Access Token (15 min): `{ sub: userId, role, loyaltyTier, jti: uuid() }`
   - Generate raw refresh token: `crypto.randomBytes(64).toString('hex')`
   - Store `SHA-256(refreshToken)` in `refresh_tokens` table with 7-day expiry, device info, IP
   - Write `USER_LOGGED_IN` to `outbox_events` (triggers Search Service to cache user's loyalty tier)
   - Return `{ accessToken, refreshToken, user: { ... } }`

**Why 5 attempts then 30-minute lockout?**
This is the OWASP recommendation. It makes automated credential-stuffing attacks economically infeasible — an attacker can only try 5 passwords per IP per 30 minutes per account. Combined with the API Gateway's rate limit (20 auth requests / 15 min per IP), the attack surface is extremely narrow.

---

### Feature 3: Access Token Refresh with Token Rotation

**Flow:**
1. Client sends `{ refreshToken }` in request body
2. Compute `SHA-256(refreshToken)` → search `refresh_tokens` table by `token_hash`
3. Not found → 401 (token was already rotated or never existed)
4. Found → check `expires_at > NOW()` → 401 if expired
5. Fetch the user → check `is_active = true`
6. **Rotation:** In ONE transaction:
   - `DELETE FROM refresh_tokens WHERE id = <found_id>`
   - Generate new raw refresh token
   - `INSERT INTO refresh_tokens` with new hash + 7-day expiry
7. Sign a new Access Token with fresh 15-min expiry
8. Return `{ accessToken, refreshToken: <new_token> }`

**Why rotation?**
If a refresh token is stolen, the attacker can use it repeatedly for 7 days without the legitimate user knowing. With rotation, when the attacker uses the token, it is replaced. The next time the legitimate user tries to use the old token, it is gone — they get a 401, which tells them their account may be compromised. Some implementations detect this and immediately revoke ALL tokens for that user (refresh token reuse detection).

---

### Feature 4: Logout (Single Session & All Sessions)

**Single-session logout (`POST /api/v1/auth/logout`):**
1. Extract `jti` from verified JWT in Authorization header
2. Delete the refresh token associated with this session
   - The client sends the refresh token in the body OR the service deletes by userId + device fingerprint
3. Write `jti` to Redis blacklist: `SET blacklist:jti:{jti} 1 EX {remaining_seconds}`
   - `remaining_seconds = token.exp - Math.floor(Date.now() / 1000)`
4. Return `200 OK`

**All-sessions logout (`POST /api/v1/auth/logout-all`):**
1. Extract `userId` from verified JWT
2. `DELETE FROM refresh_tokens WHERE user_id = userId` — kills all active sessions
3. Write current `jti` to Redis blacklist (current token also invalidated)
4. Return `200 OK`

**Why Redis blacklist only stores until token expiry?**
After the token naturally expires, the API Gateway rejects it anyway (exp check). There is no point keeping the blacklist entry beyond that. Dynamic TTL = `exp - now` means Redis auto-cleans expired entries. If you stored them permanently, Redis would fill up with millions of expired token entries.

---

### Feature 5: Email Verification

**Verify email (`POST /api/v1/auth/verify-email`):**
1. Client sends `{ token }` from the email link
2. Compute `SHA-256(token)`
3. Find user with matching `email_verification_token` AND `email_verification_expires_at > NOW()`
4. Not found or expired → 400 Bad Request
5. Update: `email_verified = true`, clear `email_verification_token`, clear `email_verification_expires_at`
6. Write `USER_EMAIL_VERIFIED` to outbox
7. Return `200 OK`

**Resend verification (`POST /api/v1/auth/resend-verification`):**
1. Client sends `{ email }`
2. Find user by email
3. If already verified → 400 "Already verified"
4. Rate limit: check `email_verification_expires_at` — if it was set less than 2 minutes ago, reject (prevent email bombing)
5. Generate new token, hash it, update DB, send new email
6. Return `200 OK`

---

### Feature 6: Forgot Password & Password Reset

**Forgot password (`POST /api/v1/auth/forgot-password`):**
1. Client sends `{ email }`
2. **Always return `200 OK` regardless of whether email exists** — this prevents email enumeration attacks (attacker cannot discover which emails are registered)
3. If email found in DB:
   - Generate reset token: `crypto.randomBytes(32).toString('hex')`
   - Store `SHA-256(token)` in `password_reset_token`, set `password_reset_expires_at = NOW() + 1 hour`
   - Send reset email with link: `https://skyhub.com/reset-password?token=<raw_token>`
4. Return `200 OK { message: "If that email exists, a reset link was sent" }`

**Reset password (`POST /api/v1/auth/reset-password`):**
1. Client sends `{ token, newPassword }`
2. Validate new password meets complexity rules
3. Compute `SHA-256(token)` → find user with matching `password_reset_token` AND `password_reset_expires_at > NOW()`
4. Not found or expired → 400 "Reset link is invalid or has expired"
5. Hash new password with bcrypt
6. In ONE transaction:
   - Update `password_hash`, clear reset token fields
   - `DELETE FROM refresh_tokens WHERE user_id = userId` — log out all devices (security measure)
7. (Optional) Send "Your password was changed" notification email
8. Return `200 OK`

**Why invalidate all sessions on password reset?**
If an attacker changed the password, the legitimate user would immediately get logged out on all devices — they notice. If the legitimate user changed the password (possibly because they suspected compromise), all the attacker's sessions are killed.

---

### Feature 7: Change Password (Authenticated)

**`POST /api/v1/auth/change-password` — requires valid JWT:**
1. Extract `userId` from JWT
2. Client sends `{ currentPassword, newPassword }`
3. Fetch user from DB
4. `bcrypt.compare(currentPassword, passwordHash)` → 401 if wrong
5. Validate `newPassword` meets complexity rules
6. Check `newPassword !== currentPassword` → 400 "New password must be different"
7. Hash new password with bcrypt
8. Update `password_hash` in DB
9. Delete all OTHER refresh tokens (keep current session active), OR delete all (force re-login)
10. Return `200 OK`

---

### Feature 8: Profile Management

**Get profile (`GET /api/v1/auth/me`) — requires valid JWT:**
1. Extract `userId` from JWT header (`X-User-Id` injected by Gateway)
2. Fetch user by ID
3. Return user profile (exclude `passwordHash`, token fields)

**Update profile (`PUT /api/v1/auth/me`) — requires valid JWT:**
1. Client sends `{ name }` (only name is user-editable; email changes require re-verification)
2. Validate: `name` min 2 chars, max 100 chars
3. Update `name` in DB
4. Return updated profile

---

### Feature 9: Loyalty Tier System

**Loyalty tiers drive flight discounts in the Search Service.**

| Tier | Booking Threshold | Discount Applied by Search Service |
|---|---|---|
| SILVER | 0 – 4 completed bookings | 5% |
| GOLD | 5 – 14 completed bookings | 10% |
| PLATINUM | 15+ completed bookings | 15% |

**How `booking_count` is incremented:**
The Booking Service publishes a `BOOKING_COMPLETED` Kafka event when a booking is confirmed. The User Service has a Kafka consumer listening to `booking-events` topic. When received, it increments `users.booking_count` for that `userId` and recalculates the tier.

**Tier upgrade logic (in `loyalty.service.ts`):**
```
calculateTier(bookingCount: number): LoyaltyTier
  bookingCount >= 15  → PLATINUM
  bookingCount >= 5   → GOLD
  default             → SILVER
```

When a tier changes, publish `USER_LOYALTY_UPDATED` event to Kafka via outbox. The Search Service consumer updates its local MongoDB cache, ensuring future searches reflect the new discount without any HTTP call.

---

### Feature 10: Role-Based Access Control (RBAC)

Three roles with distinct permissions:

| Role | Capabilities |
|---|---|
| `CUSTOMER` | Register, login, search flights, create bookings, view own bookings, manage own profile |
| `FLIGHT_ADMIN` | All CUSTOMER permissions + create/update/delete flights and schedules |
| `SUPER_ADMIN` | All permissions + view all users, change user roles, ban/unban accounts, view audit logs |

**How roles are enforced:**
- The JWT `role` claim is set at registration (default `CUSTOMER`) or by `SUPER_ADMIN` at a management endpoint
- The API Gateway injects `X-User-Role` header from the verified JWT
- Each downstream service's route middleware reads `X-User-Role` and rejects requests that don't meet the minimum role

**Database seeding:** On first startup, if no `SUPER_ADMIN` exists, the seed script creates one using credentials from environment variables (never hardcoded).

---

### Feature 11: Kafka Event Publishing (Outbox Pattern)

Events published by the User Service:

| Event Type | Trigger | Payload |
|---|---|---|
| `USER_REGISTERED` | Successful registration | `{ userId, role, loyaltyTier }` |
| `USER_EMAIL_VERIFIED` | Email verification completed | `{ userId }` |
| `USER_LOGGED_IN` | Successful login | `{ userId, loyaltyTier }` |
| `USER_LOYALTY_UPDATED` | Booking count crosses a tier threshold | `{ userId, previousTier, newTier }` |

**All events use the Outbox Pattern:**
- Event written to `outbox_events` table in the same DB transaction as the business write
- Background `OutboxWorker` polls `outbox_events` every 5 seconds
- Publishes to Kafka topic `user-identity-events`
- Marks event as `PUBLISHED`

This guarantees **exactly-once semantics**: if the service crashes after writing to the DB but before publishing to Kafka, the OutboxWorker will pick up and publish the pending event on restart.

---

### Feature 12: JWKS Endpoint (Public Key Distribution)

**`GET /.well-known/jwks.json` — public, no auth:**

Returns the RS256 public key in JWKS format so any service can independently verify tokens:

```json
{
  "keys": [
    {
      "kty": "RSA",
      "use": "sig",
      "alg": "RS256",
      "kid": "skyhub-key-v1",
      "n": "<base64url-encoded-modulus>",
      "e": "AQAB"
    }
  ]
}
```

The API Gateway fetches this once on startup, caches the key in memory, and refreshes it every 24 hours. No JWT verification requires a live call to the User Service.

---

### Feature 13: Health Check

**`GET /health` — public, no auth:**

```json
{
  "status": "healthy",
  "service": "user-service",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database": "ok",
    "redis": "ok",
    "kafka": "ok"
  }
}
```

Returns `200` if all checks pass, `503` if any check fails. Used by the Load Balancer for readiness probes.

---

## 3. Database Design & Prisma Schema

### 3.1 Entity-Relationship Diagram

```
┌─────────────────────────────────────────────────────────┐
│                         USERS                           │
├─────────────────────────────────────────────────────────┤
│ id                        UUID         PK               │
│ email                     VARCHAR(255) UNIQUE INDEX     │
│ password_hash             VARCHAR(255) NOT NULL         │
│ name                      VARCHAR(255) NOT NULL         │
│ role                      ENUM         DEFAULT CUSTOMER │
│ loyalty_tier              ENUM         DEFAULT SILVER   │
│ booking_count             INT          DEFAULT 0        │
│ is_active                 BOOLEAN      DEFAULT true     │
│ email_verified            BOOLEAN      DEFAULT false    │
│ email_verification_token  VARCHAR(64)  NULL UNIQUE      │
│ email_verification_expires_at TIMESTAMPTZ  NULL          │
│ password_reset_token      VARCHAR(64)  NULL UNIQUE      │
│ password_reset_expires_at TIMESTAMPTZ  NULL             │
│ failed_login_attempts     INT          DEFAULT 0        │
│ locked_until              TIMESTAMPTZ  NULL             │
│ last_login_at             TIMESTAMPTZ  NULL             │
│ created_at                TIMESTAMPTZ  DEFAULT NOW()    │
│ updated_at                TIMESTAMPTZ  AUTO UPDATE      │
└────────────────────────┬────────────────────────────────┘
                         │ 1
                         │
                         │ has many
                         │
                         │ N
┌────────────────────────▼────────────────────────────────┐
│                    REFRESH_TOKENS                        │
├─────────────────────────────────────────────────────────┤
│ id          UUID         PK                             │
│ user_id     UUID         FK → users.id ON DELETE CASCADE│
│ token_hash  VARCHAR(64)  UNIQUE  (SHA-256 of raw token) │
│ device_info VARCHAR(500) NULL                           │
│ ip_address  VARCHAR(45)  NULL   (IPv6 max length)       │
│ expires_at  TIMESTAMPTZ  NOT NULL                       │
│ created_at  TIMESTAMPTZ  DEFAULT NOW()                  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                    OUTBOX_EVENTS                         │
├─────────────────────────────────────────────────────────┤
│ id           UUID         PK                            │
│ event_type   VARCHAR(100) NOT NULL                      │
│ payload      JSONB        NOT NULL                      │
│ status       ENUM         DEFAULT PENDING               │
│ created_at   TIMESTAMPTZ  DEFAULT NOW()                 │
│ published_at TIMESTAMPTZ  NULL                          │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Column-by-Column Justification

#### `users` table

| Column | Type | Why This Design Choice |
|---|---|---|
| `id` | UUID v4 | Auto-increment integers expose row count (business intelligence leak) and cause conflicts in DB migrations or merges. UUIDs are globally unique. |
| `email` | VARCHAR(255) | RFC 5321 max email length is 254 chars. B-Tree indexed because every login, registration-check, and password-reset lookup is by email. |
| `password_hash` | VARCHAR(255) | Bcrypt output is always 60 characters. VARCHAR(255) is safe headroom. Never store plaintext. |
| `role` | ENUM | Enum prevents invalid values at the DB level — a code bug cannot accidentally write `"ADMIN"` instead of `"SUPER_ADMIN"`. |
| `loyalty_tier` | ENUM | Same reasoning. Enum enforces the contract at DB level. |
| `booking_count` | INT | Maintained by the User Service's Kafka consumer listening to the Booking Service. Used to trigger automatic tier upgrades. |
| `is_active` | BOOLEAN DEFAULT true | Soft ban — deactivate an account without destroying data. Important for audit trails and potential account recovery. |
| `email_verified` | BOOLEAN DEFAULT false | Gates login. An unverified account cannot generate JWTs. Prevents disposable email registrations. |
| `email_verification_token` | VARCHAR(64) UNIQUE NULL | Stores SHA-256 hash (32 bytes = 64 hex chars) of the raw verification token. NULL when not pending verification. UNIQUE prevents two users accidentally getting the same token hash. |
| `email_verification_expires_at` | TIMESTAMPTZ NULL | Verification links expire in 24 hours. Prevents stale links from being used years later. |
| `password_reset_token` | VARCHAR(64) NULL UNIQUE | Same hashing approach as verification token. NULL when no reset is pending. |
| `password_reset_expires_at` | TIMESTAMPTZ NULL | Reset links expire in 1 hour — tighter window than email verification because a reset link in an attacker's hands grants full account access. |
| `failed_login_attempts` | INT DEFAULT 0 | Incremented on wrong password, reset on success. Triggers lockout at 5. |
| `locked_until` | TIMESTAMPTZ NULL | The precise moment the lockout expires. NULL when not locked. Service checks `locked_until > NOW()` on every login attempt. |
| `last_login_at` | TIMESTAMPTZ NULL | Security auditing. Useful for "Your account was accessed from a new location" warnings. Also used for inactive account cleanup. |
| `created_at` | TIMESTAMPTZ | `TIMESTAMPTZ` stores timezone offset, essential for a global platform. Do NOT use bare `TIMESTAMP` which loses timezone info. |
| `updated_at` | TIMESTAMPTZ | Prisma `@updatedAt` auto-sets this on every UPDATE. |

#### `refresh_tokens` table

| Column | Type | Why This Design Choice |
|---|---|---|
| `token_hash` | VARCHAR(64) UNIQUE | SHA-256 of the raw 128-char hex token. 256-bit hash = 32 bytes = 64 hex chars. UNIQUE index enables O(1) exact-match lookup: `WHERE token_hash = ?` |
| `device_info` | VARCHAR(500) NULL | Stores User-Agent string (parsed). Allows "manage your devices" feature. Truncated at 500 to prevent DB bloat from enormous User-Agent headers. |
| `ip_address` | VARCHAR(45) NULL | IPv6 max length is 39 chars. VARCHAR(45) gives headroom. Used for security audit: "new login from 192.168.x.x". |
| `expires_at` | TIMESTAMPTZ | Token expiry. Checked on every `/refresh` call. Should also run a periodic cleanup job deleting rows where `expires_at < NOW()`. |
| No `updated_at` | — | Refresh tokens are immutable. Created once, deleted on use (rotation) or on logout. Never updated. |

#### `outbox_events` table

| Column | Type | Why This Design Choice |
|---|---|---|
| `event_type` | VARCHAR(100) | String type names like `USER_REGISTERED`. Avoids ENUM here — you want to add new event types without a DB migration. |
| `payload` | JSONB | Binary JSON in PostgreSQL. Indexed, queryable, and compact. Stores the full Kafka message envelope. |
| `status` | ENUM(PENDING, PUBLISHED, FAILED) | PENDING items are picked up by the OutboxWorker. FAILED items need ops attention. PUBLISHED items are archived. |
| Compound index on `(status, created_at)` | — | The OutboxWorker query is: `WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 100`. This index makes it instant. |

### 3.3 Complete Prisma Schema

**File: `services/user-service/prisma/schema.prisma`**

```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

// ─── Enums ────────────────────────────────────────────────────────────────────

enum UserRole {
  CUSTOMER
  FLIGHT_ADMIN
  SUPER_ADMIN
}

enum LoyaltyTier {
  SILVER
  GOLD
  PLATINUM
}

enum OutboxStatus {
  PENDING
  PUBLISHED
  FAILED
}

// ─── Models ───────────────────────────────────────────────────────────────────

model User {
  id                         String      @id @default(uuid())
  email                      String      @unique
  passwordHash               String      @map("password_hash")
  name                       String
  role                       UserRole    @default(CUSTOMER)
  loyaltyTier                LoyaltyTier @default(SILVER)     @map("loyalty_tier")
  bookingCount               Int         @default(0)          @map("booking_count")
  isActive                   Boolean     @default(true)       @map("is_active")
  emailVerified              Boolean     @default(false)      @map("email_verified")
  emailVerificationToken     String?     @unique              @map("email_verification_token")
  emailVerificationExpiresAt DateTime?                        @map("email_verification_expires_at")
  passwordResetToken         String?     @unique              @map("password_reset_token")
  passwordResetExpiresAt     DateTime?                        @map("password_reset_expires_at")
  failedLoginAttempts        Int         @default(0)          @map("failed_login_attempts")
  lockedUntil                DateTime?                        @map("locked_until")
  lastLoginAt                DateTime?                        @map("last_login_at")
  createdAt                  DateTime    @default(now())      @map("created_at")
  updatedAt                  DateTime    @updatedAt           @map("updated_at")

  refreshTokens RefreshToken[]

  @@index([email])
  @@map("users")
}

model RefreshToken {
  id         String   @id @default(uuid())
  userId     String   @map("user_id")
  tokenHash  String   @unique  @map("token_hash")
  deviceInfo String?  @map("device_info")
  ipAddress  String?  @map("ip_address")
  expiresAt  DateTime @map("expires_at")
  createdAt  DateTime @default(now()) @map("created_at")

  user User @relation(fields: [userId], references: [id], onDelete: Cascade)

  @@index([userId])
  @@map("refresh_tokens")
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

### 3.4 Database Indexes Summary

| Table | Index | Type | Purpose |
|---|---|---|---|
| `users` | `email` | B-Tree UNIQUE | Login, registration duplicate check, password reset lookups |
| `users` | `email_verification_token` | B-Tree UNIQUE | Email verification lookup |
| `users` | `password_reset_token` | B-Tree UNIQUE | Password reset lookup |
| `refresh_tokens` | `token_hash` | B-Tree UNIQUE | Refresh token lookup on every `/refresh` call |
| `refresh_tokens` | `user_id` | B-Tree | Find all tokens for a user on logout-all |
| `outbox_events` | `(status, created_at)` | Composite B-Tree | Outbox worker polling query |

---

## 4. Security Architecture

### 4.1 Password Hashing — bcrypt

Never store plaintext passwords. bcrypt is the industry standard because it is intentionally slow.

**Why bcrypt over SHA-256 / MD5?**
- SHA-256 can compute 10 billion hashes/second on a modern GPU
- bcrypt at cost factor 12 computes ~4 hashes/second on the same GPU
- This makes bulk offline dictionary attacks computationally infeasible

**The cost factor (rounds = 12):**
- Cost 10 → ~65ms per hash
- Cost 12 → ~250ms per hash (our choice — imperceptible to users, devastating for attackers)
- Cost 14 → ~1000ms — too slow for login

```
Hash format: $2b$12$<22-char-base64-salt><31-char-hash>
Total length: 60 characters
```

bcrypt automatically generates a unique random salt per hash. Two identical passwords produce different hashes. Rainbow table attacks are impossible.

### 4.2 RS256 JWT — Asymmetric Signing

**Key pair generation (run once, store in environment):**
```bash
openssl genrsa -out private.pem 2048
openssl rsa -in private.pem -pubout -out public.pem
```

**JWT payload (minimal — no PII):**
```json
{
  "sub":         "7b58c281-a5bf-4050-a922-a72a1cd40a92",
  "role":        "CUSTOMER",
  "loyaltyTier": "GOLD",
  "jti":         "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "iat":         1782500000,
  "exp":         1782500900
}
```

`sub` = userId. `jti` = unique token ID (used for blacklisting). No email, no name — minimise PII in tokens.

**Why `jose` library instead of `jsonwebtoken`?**
- `jsonwebtoken` has no RS256 JWKS support built in
- `jose` is the modern IETF-spec compliant library, actively maintained, supports JWKS key fetching, key rotation, and all JWT/JWK operations

### 4.3 Refresh Token Security

**Generation:**
```
raw_token = crypto.randomBytes(64).toString('hex')  → 128 hex characters
stored_hash = SHA-256(raw_token)                    → 64 hex characters (stored in DB)
```

**Verification:**
```
incoming_hash = SHA-256(token_from_request)
query: SELECT * FROM refresh_tokens WHERE token_hash = incoming_hash
```

If DB is leaked: attacker has SHA-256 hashes. Without the original 128-char token, they are useless. SHA-256 is one-way — you cannot reverse it to get the raw token.

### 4.4 Password Strength Rules

```
Minimum 8 characters
Maximum 128 characters (prevents DoS via extremely long passwords bcrypt-hashing)
Must contain at least one: uppercase letter, lowercase letter, digit, special character
Cannot be the same as the current password (on change-password endpoint)
```

### 4.5 Account Lockout

```
Threshold:   5 consecutive failed login attempts
Lock period: 30 minutes
Counter:     Reset to 0 on any successful login
```

**Timing-safe comparison:** Always run `bcrypt.compare` even for non-existent users (with a dummy hash) to prevent timing attacks that reveal whether an email is registered:
```typescript
// If user not found, compare against a dummy hash anyway
// This takes ~200ms regardless, preventing timing-based email enumeration
const dummyHash = '$2b$12$invalidhashthatisjustfillerdata';
await bcrypt.compare(password, user?.passwordHash ?? dummyHash);
// Then check if user actually existed
```

---

## 5. Complete REST API Specification

All endpoints are prefixed with `/api/v1` at the Gateway level. Internally the User Service listens on `/api/v1/auth` routes.

### Standard Response Envelope

Every response, success or error, uses this shape:

```typescript
// Success
{
  success: true,
  message: string,
  data: object | null,
  traceId: string         // X-Correlation-ID from request header
}

// Error
{
  success: false,
  error: {
    code: string,         // machine-readable error code
    message: string,      // human-readable message
    details?: Array<{ field: string, message: string }>  // Zod validation errors
  },
  traceId: string
}
```

---

### Endpoint 1: POST /api/v1/auth/register

**Auth required:** No  
**Rate limit:** 20 requests / 15 min per IP (enforced at Gateway)

**Request:**
```json
{
  "name": "John Doe",
  "email": "john.doe@example.com",
  "password": "SecurePass1!"
}
```

**Validations (Zod):**
```
name:     string, min 2, max 100, trim whitespace
email:    valid email format, lowercase, trim
password: string, min 8, max 128,
          regex: must contain uppercase + lowercase + digit + special char
```

**Success Response — 201 Created:**
```json
{
  "success": true,
  "message": "Registration successful. Please check your email to verify your account.",
  "data": {
    "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
    "name": "John Doe",
    "email": "john.doe@example.com",
    "role": "CUSTOMER",
    "loyaltyTier": "SILVER",
    "emailVerified": false
  },
  "traceId": "tr-f47ac10b-58cc-4372"
}
```

**Error Responses:**
```
400 VALIDATION_ERROR    → name/email/password failed Zod schema
409 CONFLICT            → email already registered
500 INTERNAL_ERROR      → unexpected server error
```

---

### Endpoint 2: POST /api/v1/auth/verify-email

**Auth required:** No

**Request:**
```json
{ "token": "a3f2b1c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2" }
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Email verified successfully. You can now log in.",
  "data": null,
  "traceId": "..."
}
```

**Error Responses:**
```
400 INVALID_TOKEN       → token not found or expired
400 ALREADY_VERIFIED    → account is already verified
```

---

### Endpoint 3: POST /api/v1/auth/resend-verification

**Auth required:** No

**Request:**
```json
{ "email": "john.doe@example.com" }
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "If your account exists and is unverified, a new verification email has been sent.",
  "data": null,
  "traceId": "..."
}
```

Always returns 200 regardless of whether the email exists (prevents enumeration). Rate-limited to 1 resend per 2 minutes per account.

---

### Endpoint 4: POST /api/v1/auth/login

**Auth required:** No  
**Rate limit:** 20 requests / 15 min per IP (enforced at Gateway)

**Request:**
```json
{
  "email": "john.doe@example.com",
  "password": "SecurePass1!"
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Login successful.",
  "data": {
    "user": {
      "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
      "name": "John Doe",
      "email": "john.doe@example.com",
      "role": "CUSTOMER",
      "loyaltyTier": "GOLD",
      "emailVerified": true,
      "lastLoginAt": "2026-05-28T09:00:00.000Z"
    },
    "tokens": {
      "accessToken": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
      "refreshToken": "a3f2b1c4...128-hex-chars...",
      "accessTokenExpiresAt": "2026-05-28T10:15:00.000Z",
      "refreshTokenExpiresAt": "2026-06-04T10:00:00.000Z"
    }
  },
  "traceId": "..."
}
```

**Error Responses:**
```
400 VALIDATION_ERROR         → invalid email/password format
401 UNAUTHORIZED             → wrong password
401 EMAIL_NOT_VERIFIED       → account not yet verified
401 ACCOUNT_INACTIVE         → account was deactivated
423 ACCOUNT_LOCKED           → {
                                  "code": "ACCOUNT_LOCKED",
                                  "message": "Account locked due to too many failed attempts",
                                  "lockExpiresAt": "2026-05-28T10:30:00.000Z",
                                  "secondsRemaining": 1247
                                }
```

---

### Endpoint 5: POST /api/v1/auth/refresh

**Auth required:** No (refresh token is the credential)

**Request:**
```json
{ "refreshToken": "a3f2b1c4...128-hex-chars..." }
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Access token refreshed successfully.",
  "data": {
    "accessToken": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
    "refreshToken": "b4c3d2e1...new-128-hex-chars...",
    "accessTokenExpiresAt": "2026-05-28T10:30:00.000Z",
    "refreshTokenExpiresAt": "2026-06-04T10:15:00.000Z"
  },
  "traceId": "..."
}
```

Both tokens are new. The old refresh token is deleted (rotation).

**Error Responses:**
```
400 VALIDATION_ERROR         → missing/malformed refreshToken
401 INVALID_REFRESH_TOKEN    → token not found (deleted after rotation or never existed)
401 REFRESH_TOKEN_EXPIRED    → token found but expired
401 ACCOUNT_INACTIVE         → user account deactivated
```

---

### Endpoint 6: POST /api/v1/auth/logout

**Auth required:** Yes (Bearer JWT in Authorization header)

**Request body:** Empty `{}`

**What happens internally:**
1. `jti` extracted from verified JWT (Gateway already verified it, User Service gets `X-User-Jti` header)
2. Refresh token for this session deleted from DB (client should send refresh token in body for accurate deletion, or delete by userId + device fingerprint)
3. `jti` written to Redis blacklist with TTL = remaining token lifetime

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Logged out successfully.",
  "data": null,
  "traceId": "..."
}
```

---

### Endpoint 7: POST /api/v1/auth/logout-all

**Auth required:** Yes (Bearer JWT)

**Request body:** Empty `{}`

**What happens:**
- ALL refresh tokens for this user deleted from DB
- Current JWT's `jti` added to Redis blacklist
- User must log in again on all devices

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Logged out from all sessions successfully.",
  "data": { "sessionsTerminated": 3 },
  "traceId": "..."
}
```

---

### Endpoint 8: GET /api/v1/auth/me

**Auth required:** Yes (Bearer JWT)

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Profile retrieved successfully.",
  "data": {
    "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
    "name": "John Doe",
    "email": "john.doe@example.com",
    "role": "CUSTOMER",
    "loyaltyTier": "GOLD",
    "bookingCount": 7,
    "emailVerified": true,
    "isActive": true,
    "lastLoginAt": "2026-05-28T09:00:00.000Z",
    "createdAt": "2025-01-15T08:30:00.000Z"
  },
  "traceId": "..."
}
```

Never return: `passwordHash`, token fields, `failedLoginAttempts`, `lockedUntil`.

---

### Endpoint 9: PUT /api/v1/auth/me

**Auth required:** Yes (Bearer JWT)

**Request:**
```json
{ "name": "John Smith" }
```

**Validations:** `name`: string, min 2, max 100, trim

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Profile updated successfully.",
  "data": {
    "userId": "...",
    "name": "John Smith",
    "email": "john.doe@example.com",
    "role": "CUSTOMER",
    "loyaltyTier": "GOLD"
  },
  "traceId": "..."
}
```

---

### Endpoint 10: POST /api/v1/auth/change-password

**Auth required:** Yes (Bearer JWT)

**Request:**
```json
{
  "currentPassword": "SecurePass1!",
  "newPassword": "EvenBetter2@"
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Password changed successfully. Please log in again on your other devices.",
  "data": null,
  "traceId": "..."
}
```

**Error Responses:**
```
400 VALIDATION_ERROR        → new password fails complexity rules
400 SAME_PASSWORD           → new password identical to current
401 WRONG_CURRENT_PASSWORD  → bcrypt.compare failed
```

---

### Endpoint 11: POST /api/v1/auth/forgot-password

**Auth required:** No

**Request:**
```json
{ "email": "john.doe@example.com" }
```

**Success Response — 200 OK (always, regardless of whether email exists):**
```json
{
  "success": true,
  "message": "If an account with that email exists, a password reset link has been sent.",
  "data": null,
  "traceId": "..."
}
```

This response is always identical. Never reveal whether an email is registered.

---

### Endpoint 12: POST /api/v1/auth/reset-password

**Auth required:** No

**Request:**
```json
{
  "token": "a3f2b1c4...64-hex-chars...",
  "newPassword": "NewSecurePass3#"
}
```

**Success Response — 200 OK:**
```json
{
  "success": true,
  "message": "Password reset successfully. Please log in with your new password.",
  "data": null,
  "traceId": "..."
}
```

**Error Responses:**
```
400 INVALID_RESET_TOKEN     → token not found or expired
400 VALIDATION_ERROR        → newPassword fails complexity rules
```

---

### Endpoint 13: GET /.well-known/jwks.json

**Auth required:** No  
**Path note:** This is NOT under `/api/v1/` prefix — it is a well-known URI standard

**Success Response — 200 OK:**
```json
{
  "keys": [
    {
      "kty": "RSA",
      "use": "sig",
      "alg": "RS256",
      "kid": "skyhub-key-v1",
      "n": "0vx7agoebGcQSuu...",
      "e": "AQAB"
    }
  ]
}
```

**Cache headers:** `Cache-Control: public, max-age=86400` — clients/gateways may cache this for 24 hours.

---

### Endpoint 14: GET /health

**Auth required:** No

**Healthy Response — 200 OK:**
```json
{
  "status": "healthy",
  "service": "user-service",
  "version": "1.0.0",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database": "ok",
    "redis":    "ok",
    "kafka":    "ok"
  }
}
```

**Degraded Response — 503 Service Unavailable:**
```json
{
  "status": "degraded",
  "service": "user-service",
  "timestamp": "2026-05-28T10:00:00.000Z",
  "checks": {
    "database": "ok",
    "redis":    "error: ECONNREFUSED",
    "kafka":    "ok"
  }
}
```

---

## 6. Zod Validation Schemas

**File: `src/routes/schemas/auth.schemas.ts`**

```typescript
import { z } from 'zod';

const passwordSchema = z
  .string()
  .min(8,  'Password must be at least 8 characters')
  .max(128, 'Password must not exceed 128 characters')
  .regex(
    /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?])/,
    'Password must contain at least one uppercase letter, one lowercase letter, one number, and one special character'
  );

export const RegisterSchema = z.object({
  name:     z.string().trim().min(2, 'Name must be at least 2 characters').max(100),
  email:    z.string().trim().toLowerCase().email('Invalid email format'),
  password: passwordSchema,
});

export const LoginSchema = z.object({
  email:    z.string().trim().toLowerCase().email(),
  password: z.string().min(1, 'Password is required'),
});

export const RefreshTokenSchema = z.object({
  refreshToken: z.string().min(1, 'Refresh token is required'),
});

export const VerifyEmailSchema = z.object({
  token: z.string().length(64, 'Invalid token format'),
});

export const ResendVerificationSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
});

export const ForgotPasswordSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
});

export const ResetPasswordSchema = z.object({
  token:       z.string().min(1, 'Token is required'),
  newPassword: passwordSchema,
});

export const ChangePasswordSchema = z.object({
  currentPassword: z.string().min(1, 'Current password is required'),
  newPassword:     passwordSchema,
});

export const UpdateProfileSchema = z.object({
  name: z.string().trim().min(2).max(100),
});

export type RegisterInput             = z.infer<typeof RegisterSchema>;
export type LoginInput                = z.infer<typeof LoginSchema>;
export type RefreshTokenInput         = z.infer<typeof RefreshTokenSchema>;
export type VerifyEmailInput          = z.infer<typeof VerifyEmailSchema>;
export type ResendVerificationInput   = z.infer<typeof ResendVerificationSchema>;
export type ForgotPasswordInput       = z.infer<typeof ForgotPasswordSchema>;
export type ResetPasswordInput        = z.infer<typeof ResetPasswordSchema>;
export type ChangePasswordInput       = z.infer<typeof ChangePasswordSchema>;
export type UpdateProfileInput        = z.infer<typeof UpdateProfileSchema>;
```

---

## 7. Layered Architecture & File Map

```
services/user-service/
│
├── prisma/
│   ├── schema.prisma              ← All 3 models: User, RefreshToken, OutboxEvent
│   ├── migrations/                ← Auto-generated by `prisma migrate dev`
│   │   └── 20260528_init/
│   │       └── migration.sql
│   └── seed.ts                    ← Creates SUPER_ADMIN + FLIGHT_ADMIN from env vars
│
├── src/
│   │
│   ├── config/
│   │   ├── env.ts                 ← Zod-validated env vars — crashes on startup if invalid
│   │   ├── database.ts            ← Prisma client singleton (shared across all imports)
│   │   ├── redis.ts               ← ioredis client singleton for blacklist writes
│   │   ├── kafka.ts               ← KafkaJS producer instance
│   │   ├── logger.ts              ← Pino logger with AsyncLocalStorage correlation injection
│   │   └── keys.ts                ← RSA key pair loading for JWT sign/verify + JWKS export
│   │
│   ├── repositories/
│   │   ├── user.repository.ts     ← All Prisma user queries — NO business logic here
│   │   ├── token.repository.ts    ← All Prisma refresh_token queries
│   │   └── outbox.repository.ts   ← Insert + update outbox_events
│   │
│   ├── services/
│   │   ├── auth.service.ts        ← Registration, login, logout, email verification logic
│   │   ├── token.service.ts       ← JWT sign/verify, refresh token create/rotate/delete
│   │   ├── loyalty.service.ts     ← Tier calculation, upgrade detection
│   │   └── email.service.ts       ← nodemailer wrapper for verification + reset emails
│   │
│   ├── controllers/
│   │   └── auth.controller.ts     ← HTTP layer only: parse req, call service, send res
│   │
│   ├── routes/
│   │   ├── auth.routes.ts         ← Maps HTTP verbs + paths → controller methods
│   │   ├── health.routes.ts       ← GET /health — DB + Redis + Kafka liveness checks
│   │   ├── jwks.routes.ts         ← GET /.well-known/jwks.json — public key distribution
│   │   ├── metrics.routes.ts      ← GET /metrics — Prometheus scrape endpoint
│   │   └── schemas/
│   │       └── auth.schemas.ts    ← All Zod schemas (from Section 6)
│   │
│   ├── middlewares/
│   │   ├── validate.ts            ← Generic Zod validation middleware factory
│   │   ├── requireAuth.ts         ← Reads X-User-Id + X-User-Role headers (injected by Gateway)
│   │   └── errorHandler.ts        ← Global Express error handler — formats all errors
│   │
│   ├── events/
│   │   ├── producers/
│   │   │   └── user.producer.ts   ← KafkaJS publish function for user-identity-events
│   │   ├── consumers/
│   │   │   └── booking.consumer.ts← Listens to booking-events → increments booking_count
│   │   └── outbox.worker.ts       ← Polls outbox_events every 5s, publishes to Kafka
│   │
│   ├── types/
│   │   ├── express.d.ts           ← Augments Express Request: req.userId, req.userRole, req.userJti
│   │   └── jwt.types.ts           ← JwtPayload interface (sub, role, loyaltyTier, jti, iat, exp)
│   │
│   ├── utils/
│   │   ├── crypto.utils.ts        ← hashToken(), generateRawToken(), generateJti()
│   │   └── response.utils.ts      ← sendSuccess(), sendError() helpers
│   │
│   ├── app.ts                     ← Express setup: helmet, cors, body-parser, routes
│   └── server.ts                  ← Boot: DB connect, Redis connect, Kafka connect, listen
│
├── tests/
│   ├── unit/
│   │   ├── services/
│   │   │   ├── auth.service.test.ts
│   │   │   └── token.service.test.ts
│   │   └── utils/
│   │       └── crypto.utils.test.ts
│   └── integration/
│       ├── auth.register.test.ts
│       ├── auth.login.test.ts
│       └── auth.refresh.test.ts
│
├── .env.example
├── package.json
└── tsconfig.json
```

### Layer Rules (Never Break These)

```
Routes     → calls Middlewares + Controller only
Controller → calls Services only (never Repositories or DB directly)
Services   → calls Repositories only (no req/res objects, no HTTP imports)
Repository → calls Prisma only (no business logic, no if/else rules)

Middleware → cross-cutting: validation, auth header reading, error handling
Events     → Kafka producers/consumers (called from Services, not Controllers)
```

**Why this strict layering?**
- Services become independently testable — no need to mock HTTP objects
- Repositories are swappable — replace Prisma with raw SQL without touching services
- Controllers stay thin — easy to read, easy to change routes without touching logic

---

## 8. npm Dependencies

**File: `services/user-service/package.json`**

```json
{
  "name": "@skyhub/user-service",
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
    "bcrypt":               "^5.1.1",
    "cors":                 "^2.8.5",
    "dotenv":               "^16.4.5",
    "express":              "^5.2.1",
    "helmet":               "^7.1.0",
    "ioredis":              "^5.3.2",
    "jose":                 "^5.3.0",
    "kafkajs":              "^2.2.4",
    "nodemailer":           "^6.9.13",
    "pino":                 "^9.2.0",
    "pino-http":            "^10.2.0",
    "prom-client":          "^15.1.2",
    "uuid":                 "^9.0.1",
    "zod":                  "^3.23.8"
  },
  "devDependencies": {
    "@types/bcrypt":        "^5.0.2",
    "@types/cors":          "^2.8.17",
    "@types/express":       "^5.0.6",
    "@types/nodemailer":    "^6.4.15",
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
| `@prisma/client` | Type-safe PostgreSQL ORM — generates a TypeScript client from your schema |
| `bcrypt` | Adaptive password hashing with built-in salt generation |
| `cors` | Express CORS middleware — needed so internal services can configure cross-origin policy |
| `dotenv` | Loads `.env` file into `process.env` before the Zod env schema runs |
| `express` | HTTP server framework (v5 — async errors propagate to error middleware natively, no patch needed) |
| `helmet` | Sets 7 security HTTP headers in one line |
| `jose` | Modern JWT library with RS256 / JWKS support (replaces `jsonwebtoken` for RS256) |
| `kafkajs` | Official Kafka Node.js client |
| `pino` + `pino-http` | Structured JSON logger — 5× faster than Winston, native `child()` for per-request context |
| `prom-client` | Prometheus metrics exporter — powers the `/metrics` endpoint |
| `pino-pretty` | Dev-only: pipes Pino JSON output into human-readable format (`npm run dev \| npx pino-pretty`) |
| `supertest` | Integration test HTTP client — makes requests against the Express app without a running server |
| `vitest` | Fast test runner (Vite-based) — 10× faster than Jest for TypeScript projects |

---

## 9. Environment Variables

**File: `services/user-service/.env.example`**

```bash
# ── Server ────────────────────────────────────────────────────────────
NODE_ENV=development
PORT=3001
SERVICE_NAME=user-service

# ── Database ──────────────────────────────────────────────────────────
# connection_limit: max Prisma pool connections (keep ≤ 10 for local dev)
# pool_timeout: seconds to wait for a free connection before erroring
DATABASE_URL=postgresql://skyhub:skyhub_local@localhost:5432/skyhub_user_db?connection_limit=10&pool_timeout=10

# ── Redis ─────────────────────────────────────────────────────────────
# DB 0 is shared with the Gateway for JWT blacklist entries
REDIS_URL=redis://localhost:6379/0

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=user-service
KAFKA_TOPIC_USER_EVENTS=user-identity-events
KAFKA_CONSUMER_GROUP_ID=user-service-booking-consumer

# ── JWT / RS256 Keys ──────────────────────────────────────────────────
# Paste the FULL PEM content (multiline) with escaped newlines or use a file path
JWT_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END RSA PRIVATE KEY-----"
JWT_PUBLIC_KEY="-----BEGIN PUBLIC KEY-----\nMIIB...\n-----END PUBLIC KEY-----"
JWT_KEY_ID=skyhub-key-v1
JWT_ACCESS_TOKEN_EXPIRY=15m
JWT_REFRESH_TOKEN_EXPIRY_DAYS=7

# ── Bcrypt ────────────────────────────────────────────────────────────
BCRYPT_ROUNDS=12

# ── Email (for verification + password reset) ─────────────────────────
# In dev: use Mailtrap or Ethereal Email (catches all emails, never sends real ones)
SMTP_HOST=smtp.mailtrap.io
SMTP_PORT=587
SMTP_USER=your-mailtrap-user
SMTP_PASS=your-mailtrap-password
EMAIL_FROM=noreply@skyhub.com
APP_BASE_URL=http://localhost:3000

# ── Admin Seed ────────────────────────────────────────────────────────
SUPER_ADMIN_NAME=Super Admin
SUPER_ADMIN_EMAIL=superadmin@skyhub.com
SUPER_ADMIN_PASSWORD=SuperAdmin1!@#
FLIGHT_ADMIN_NAME=Flight Admin
FLIGHT_ADMIN_EMAIL=flightadmin@skyhub.com
FLIGHT_ADMIN_PASSWORD=FlightAdmin1!@#

# ── Observability ─────────────────────────────────────────────────────
LOG_LEVEL=info
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

### Env Validation (Startup Crash-Fast)

**File: `src/config/env.ts`**

```typescript
import 'dotenv/config';   // must be first import — loads .env before anything reads process.env
import { z } from 'zod';

const envSchema = z.object({
  // ── Server ───────────────────────────────────────────────────────────
  NODE_ENV:   z.enum(['development', 'production', 'test']),
  PORT:       z.string().transform(Number).default('3001'),

  // ── Database ─────────────────────────────────────────────────────────
  DATABASE_URL: z.string().url(),

  // ── Redis ────────────────────────────────────────────────────────────
  REDIS_URL: z.string(),

  // ── Kafka ────────────────────────────────────────────────────────────
  KAFKA_BROKERS:            z.string(),
  KAFKA_CLIENT_ID:          z.string(),
  KAFKA_TOPIC_USER_EVENTS:  z.string(),
  KAFKA_CONSUMER_GROUP_ID:  z.string(),   // consumer group for booking-events listener

  // ── JWT ──────────────────────────────────────────────────────────────
  JWT_PRIVATE_KEY:             z.string().min(100),
  JWT_PUBLIC_KEY:              z.string().min(100),
  JWT_KEY_ID:                  z.string(),
  JWT_ACCESS_TOKEN_EXPIRY:     z.string().default('15m'),
  JWT_REFRESH_TOKEN_EXPIRY_DAYS: z.string().transform(Number).default('7'),

  // ── Bcrypt ───────────────────────────────────────────────────────────
  BCRYPT_ROUNDS: z.string().transform(Number).default('12'),

  // ── Email ────────────────────────────────────────────────────────────
  SMTP_HOST:    z.string(),
  SMTP_PORT:    z.string().transform(Number),
  SMTP_USER:    z.string(),
  SMTP_PASS:    z.string(),
  EMAIL_FROM:   z.string().email(),
  APP_BASE_URL: z.string().url(),

  // ── Observability ────────────────────────────────────────────────────
  LOG_LEVEL:                     z.enum(['error', 'warn', 'info', 'debug']).default('info'),
  OTEL_EXPORTER_OTLP_ENDPOINT:   z.string().url().optional(),

  // ── Seed credentials (only read by prisma/seed.ts, not at runtime) ───
  // Validated here so the service won't start with obviously wrong values,
  // but seed.ts may also be run standalone against an already-running DB.
  SUPER_ADMIN_EMAIL:    z.string().email(),
  SUPER_ADMIN_PASSWORD: z.string().min(8),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('❌ Invalid environment variables:');
  console.error(parsed.error.flatten().fieldErrors);
  process.exit(1);  // Crash immediately — never start with missing config
}

export const env = parsed.data;
```

---

## 10. Step-by-Step Build Plan

Work through these steps in order. Complete and validate each step before moving to the next. Each step has a clear validation test so you know it works before proceeding.

---

### Step 1: Project Setup & Tooling

**What to do:**
1. Update `services/user-service/package.json` with all dependencies from Section 8 and run `npm install` from the monorepo root
2. Confirm `services/user-service/tsconfig.json` extends `../../tsconfig.base.json` and references both shared packages
3. Initialize Prisma from inside the service folder:
   ```bash
   cd services/user-service
   npx prisma init
   ```
4. Replace the generated `prisma/schema.prisma` with the schema from Section 3.3
5. Create `src/config/env.ts` (Section 9 code) — this must be the very first file so everything else can import it
6. Create all other config files from Step 3: `database.ts`, `redis.ts`, `kafka.ts`, `logger.ts`, `keys.ts`
7. Generate RSA key pair (run once, store the output in `.env`):
   ```bash
   openssl genrsa -out private.pem 2048
   openssl rsa -in private.pem -pubout -out public.pem
   # Then paste the contents into .env as JWT_PRIVATE_KEY and JWT_PUBLIC_KEY
   # Replace actual newlines with \n so they fit on one line in .env
   ```
8. Copy `.env.example` to `.env` and fill in all values (use Mailtrap for SMTP in dev)

**`tsconfig.json` for user-service:**
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src"
  },
  "include": ["src/**/*", "prisma/seed.ts"],
  "references": [
    { "path": "../../packages/shared-types" },
    { "path": "../../packages/common-utils" }
  ]
}
```

**Validation:** Run `npm run typecheck` from `services/user-service/`. Zero errors. Start the service with `npm run dev` — if any env var is missing it should crash immediately with a clear field-by-field error list from Zod.

---

### Step 2: Database Migration

**What to do:**
1. Start Docker infrastructure from the monorepo root: `docker compose up -d`
2. Verify Postgres is running: `docker compose ps`
3. Run initial migration:
   ```bash
   cd services/user-service
   npx prisma migrate dev --name init
   ```
4. Verify tables: `npx prisma studio` — confirm `users`, `refresh_tokens`, `outbox_events` exist with all columns
5. Create `prisma/seed.ts`:

```typescript
import 'dotenv/config';   // seed.ts runs standalone — must load .env itself
import { PrismaClient, UserRole, LoyaltyTier } from '@prisma/client';
import bcrypt from 'bcrypt';

const prisma = new PrismaClient();

async function seed() {
  const rounds = parseInt(process.env.BCRYPT_ROUNDS ?? '12');

  const admins = [
    {
      name:  process.env.SUPER_ADMIN_NAME  ?? 'Super Admin',
      email: process.env.SUPER_ADMIN_EMAIL ?? 'superadmin@skyhub.com',
      pass:  process.env.SUPER_ADMIN_PASSWORD ?? '',
      role:  UserRole.SUPER_ADMIN,
    },
    {
      name:  process.env.FLIGHT_ADMIN_NAME  ?? 'Flight Admin',
      email: process.env.FLIGHT_ADMIN_EMAIL ?? 'flightadmin@skyhub.com',
      pass:  process.env.FLIGHT_ADMIN_PASSWORD ?? '',
      role:  UserRole.FLIGHT_ADMIN,
    },
  ];

  for (const admin of admins) {
    const existing = await prisma.user.findUnique({ where: { email: admin.email } });
    if (existing) { console.log(`${admin.email} already exists — skipping`); continue; }

    const passwordHash = await bcrypt.hash(admin.pass, rounds);
    await prisma.user.create({
      data: {
        name: admin.name, email: admin.email, passwordHash,
        role: admin.role, loyaltyTier: LoyaltyTier.SILVER,
        isActive: true, emailVerified: true,  // Admins pre-verified
      },
    });
    console.log(`Created ${admin.role}: ${admin.email}`);
  }

  await prisma.$disconnect();
}

seed().catch(console.error);
```

**Validation:** Run `npm run seed`. Open Prisma Studio — two users with `email_verified = true` should exist.

---

### Step 3: Utilities & Common Infrastructure

**What to do — create these files:**

**`src/config/database.ts`:**
```typescript
import { PrismaClient } from '@prisma/client';
import { env } from './env.js';

const globalForPrisma = globalThis as unknown as { prisma?: PrismaClient };

export const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    log: env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
  });

if (env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma;
```

**`src/config/redis.ts`:**
```typescript
import Redis from 'ioredis';
import { env } from './env.js';

export const redisClient = new Redis(env.REDIS_URL, {
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
  lazyConnect: false,
});

redisClient.on('error', (err) => {
  // Log but don't crash — connection will be retried automatically
  console.error('[Redis] connection error:', err.message);
});
```

**`src/config/kafka.ts`:**
```typescript
import { Kafka } from 'kafkajs';
import { env } from './env.js';

const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers:  env.KAFKA_BROKERS.split(','),
});

export const kafkaProducer = kafka.producer({
  allowAutoTopicCreation: false,
  transactionTimeout:     30000,
});
```

**`src/config/logger.ts`:**
```typescript
import pino from 'pino';
import { AsyncLocalStorage } from 'async_hooks';
import { env } from './env.js';

interface RequestContext {
  correlationId?: string;
  userId?:        string;
}

// One storage per process — carries correlationId + userId through async chains
export const asyncStorage = new AsyncLocalStorage<RequestContext>();

export const logger = pino({
  level: env.LOG_LEVEL,
  base:  { service: 'user-service' },
  timestamp: pino.stdTimeFunctions.isoTime,
  mixin() {
    // Automatically injected into every log line without explicit passing
    return asyncStorage.getStore() ?? {};
  },
});
```

> **Local dev tip:** Pino outputs machine-readable JSON. Pipe through pino-pretty for human-readable output:
> ```bash
> npm run dev | npx pino-pretty
> ```

**`src/config/keys.ts`:**
```typescript
import { importPKCS8, importSPKI, exportJWK } from 'jose';
import { env } from './env.js';

let _privateKey: Awaited<ReturnType<typeof importPKCS8>>;
let _publicJwk: Record<string, unknown>;

export async function loadKeys(): Promise<void> {
  // Env vars may have literal \n — replace with real newlines
  const privateKey = await importPKCS8(env.JWT_PRIVATE_KEY.replace(/\\n/g, '\n'), 'RS256');
  const publicKey  = await importSPKI(env.JWT_PUBLIC_KEY.replace(/\\n/g, '\n'),   'RS256');
  const jwk        = await exportJWK(publicKey);

  _privateKey = privateKey;
  _publicJwk  = { ...jwk, use: 'sig', alg: 'RS256', kid: env.JWT_KEY_ID };
}

export function getPrivateKey() {
  if (!_privateKey) throw new Error('Keys not loaded — call loadKeys() in bootstrap');
  return _privateKey;
}

export function getPublicJwk() {
  if (!_publicJwk) throw new Error('Keys not loaded — call loadKeys() in bootstrap');
  return _publicJwk;
}
```

**`src/services/email.service.ts`:**
```typescript
import nodemailer from 'nodemailer';
import { env } from '../config/env.js';

const transporter = nodemailer.createTransport({
  host: env.SMTP_HOST,
  port: env.SMTP_PORT,
  auth: { user: env.SMTP_USER, pass: env.SMTP_PASS },
});

export const emailService = {
  async sendVerificationEmail(to: string, rawToken: string): Promise<void> {
    const link = `${env.APP_BASE_URL}/verify-email?token=${rawToken}`;
    await transporter.sendMail({
      from:    env.EMAIL_FROM,
      to,
      subject: 'Verify your SkyHub account',
      html:    `<p>Click <a href="${link}">here</a> to verify your email. This link expires in 24 hours.</p>`,
    });
  },

  async sendPasswordResetEmail(to: string, rawToken: string): Promise<void> {
    const link = `${env.APP_BASE_URL}/reset-password?token=${rawToken}`;
    await transporter.sendMail({
      from:    env.EMAIL_FROM,
      to,
      subject: 'Reset your SkyHub password',
      html:    `<p>Click <a href="${link}">here</a> to reset your password. This link expires in 1 hour.</p>`,
    });
  },
};
```

> **Local dev email:** Use [Mailtrap](https://mailtrap.io) or [Ethereal Email](https://ethereal.email) for SMTP credentials. These catch all emails without sending real ones — perfect for development.

**`src/utils/crypto.utils.ts`:**
```typescript
import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';

export function generateRawToken(bytes = 64): string {
  return crypto.randomBytes(bytes).toString('hex');  // 128 hex chars for 64 bytes
}

export function hashToken(rawToken: string): string {
  return crypto.createHash('sha256').update(rawToken).digest('hex');  // 64 hex chars
}

export function generateJti(): string {
  return uuidv4();
}
```

**`src/utils/response.utils.ts`:**
```typescript
import { Response } from 'express';

interface SuccessOptions {
  res:        Response;
  statusCode: number;
  message:    string;
  data?:      unknown;
  traceId:    string;
}

interface ErrorOptions {
  res:        Response;
  statusCode: number;
  code:       string;
  message:    string;
  details?:   Array<{ field: string; message: string }>;
  traceId:    string;
}

export function sendSuccess({ res, statusCode, message, data = null, traceId }: SuccessOptions) {
  res.status(statusCode).json({ success: true, message, data, traceId });
}

export function sendError({ res, statusCode, code, message, details, traceId }: ErrorOptions) {
  res.status(statusCode).json({ success: false, error: { code, message, details }, traceId });
}
```

**`src/middlewares/validate.ts`:**
```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodSchema } from 'zod';
import { sendError } from '../utils/response.utils';

export function validate(schema: ZodSchema) {
  return (req: Request, res: Response, next: NextFunction) => {
    const result = schema.safeParse(req.body);
    if (!result.success) {
      const details = result.error.errors.map(e => ({
        field: e.path.join('.'),
        message: e.message,
      }));
      return sendError({
        res, statusCode: 400, code: 'VALIDATION_ERROR',
        message: 'Request validation failed', details,
        traceId: req.headers['x-correlation-id'] as string ?? '',
      });
    }
    req.body = result.data;  // Replace with parsed + sanitised data
    next();
  };
}
```

**`src/middlewares/requireAuth.ts`:**
```typescript
import { Request, Response, NextFunction } from 'express';
import { sendError } from '../utils/response.utils';

// The API Gateway has already verified the JWT and injected these headers.
// This middleware just reads them and attaches them to req for use by controllers.
export function requireAuth(req: Request, res: Response, next: NextFunction) {
  const userId   = req.headers['x-user-id']   as string;
  const userRole = req.headers['x-user-role'] as string;
  const userJti  = req.headers['x-user-jti']  as string;  // needed for JWT blacklisting on logout

  if (!userId || !userRole) {
    return sendError({
      res, statusCode: 401, code: 'UNAUTHORIZED',
      message: 'Authentication required',
      traceId: req.headers['x-correlation-id'] as string ?? '',
    });
  }

  req.userId   = userId;
  req.userRole = userRole;
  req.userJti  = userJti;
  next();
}
```

**`src/types/express.d.ts`:**
```typescript
declare namespace Express {
  interface Request {
    userId?:   string;
    userRole?: string;
    userJti?:  string;    // JWT ID — injected by Gateway, used for blacklisting on logout
  }
}
```

**`src/types/jwt.types.ts`:**
```typescript
export interface JwtPayload {
  sub:         string;        // userId
  role:        string;        // CUSTOMER | FLIGHT_ADMIN | SUPER_ADMIN
  loyaltyTier: string;        // SILVER | GOLD | PLATINUM
  jti:         string;        // unique token ID — used for blacklisting
  iat:         number;
  exp:         number;
}
```

> **`AppError` class** — `errorHandler.ts` imports `AppError` from `@skyhub/common-utils`, which is currently an empty stub. Before building the User Service you must implement this class in `packages/common-utils/src/index.ts`:
>
> ```typescript
> export class AppError extends Error {
>   constructor(
>     public readonly statusCode: number,
>     public readonly code: string,
>     message: string,
>   ) {
>     super(message);
>     this.name = 'AppError';
>   }
> }
> ```
> Run `npm run build` from the `common-utils` package after adding this so the dist is up to date.

**`src/middlewares/errorHandler.ts`:**
```typescript
import { Request, Response, NextFunction } from 'express';
import { AppError } from '@skyhub/common-utils';
import { logger } from '../config/logger.js';

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

**Validation:** `npm run typecheck` — zero errors.

---

### Step 4: Repository Layer

**What to do — implement data access, NO business logic:**

**`src/repositories/user.repository.ts`** — key methods:
```typescript
findByEmail(email: string): Promise<User | null>
findById(id: string): Promise<User | null>
create(data: CreateUserData): Promise<User>
updateById(id: string, data: Partial<User>): Promise<User>
incrementFailedAttempts(id: string): Promise<void>
resetFailedAttempts(id: string): Promise<void>
findByEmailVerificationToken(tokenHash: string): Promise<User | null>
findByPasswordResetToken(tokenHash: string): Promise<User | null>
```

**`src/repositories/token.repository.ts`** — key methods:
```typescript
create(data: CreateTokenData): Promise<RefreshToken>
findByHash(tokenHash: string): Promise<RefreshToken | null>
deleteById(id: string): Promise<void>
deleteAllByUserId(userId: string): Promise<number>  // returns count
deleteExpired(): Promise<number>
```

**`src/repositories/outbox.repository.ts`** — key methods:
```typescript
create(eventType: string, payload: object): Promise<void>
getPending(limit: number): Promise<OutboxEvent[]>
markPublished(id: string): Promise<void>
markFailed(id: string): Promise<void>
```

**Rule:** Every method in a repository is a simple Prisma call. No if/else. No calculations. Just SQL. This is the only layer that imports `prisma`.

**Validation:** Write a quick test script that calls `userRepository.create(...)` and logs the result. Check Prisma Studio to confirm the row exists.

---

### Step 5: Service Layer

**What to do — implement business logic, NO HTTP:**

**`src/services/token.service.ts`** — core logic:
```typescript
// Signing
async signAccessToken(userId, role, loyaltyTier): Promise<{ token: string, jti: string, expiresAt: Date }>
  → jose.SignJWT({ sub: userId, role, loyaltyTier, jti: generateJti() })
       .setProtectedHeader({ alg: 'RS256', kid: env.JWT_KEY_ID })
       .setExpirationTime('15m')
       .sign(privateKey)

// Creating a refresh token record
async createRefreshToken(userId, deviceInfo, ipAddress): Promise<string>
  → rawToken = generateRawToken(64)
  → tokenHash = hashToken(rawToken)
  → tokenRepository.create({ userId, tokenHash, deviceInfo, ipAddress, expiresAt: +7 days })
  → return rawToken  (only returned ONCE — caller sends to client)

// Rotating a refresh token
async rotateRefreshToken(oldTokenRaw, deviceInfo, ipAddress): Promise<{ newRaw, jti, accessToken, ... }>
  → hash = hashToken(oldTokenRaw)
  → existing = tokenRepository.findByHash(hash)
  → if not found → throw AppError(401, INVALID_REFRESH_TOKEN)
  → if expired → throw AppError(401, REFRESH_TOKEN_EXPIRED)
  → tokenRepository.deleteById(existing.id)
  → return createRefreshToken + signAccessToken for existing.userId
```

**`src/services/auth.service.ts`** — core logic:
```typescript
async register(name, email, password): ...
  → check duplicate email
  → hash password
  → generate verification token + hash
  → DB transaction: create user + create outbox event
  → send verification email
  → return user profile

async login(email, password, deviceInfo, ip): ...
  → find user by email (timing-safe)
  → check is_active, email_verified, locked_until
  → bcrypt.compare
  → handle wrong password (increment counter, possibly lock)
  → create access token + refresh token
  → update last_login_at
  → write USER_LOGGED_IN to outbox
  → return tokens + user

async verifyEmail(rawToken): ...
async forgotPassword(email): ...
async resetPassword(rawToken, newPassword): ...
async changePassword(userId, currentPassword, newPassword): ...
async logout(userId, jti, rawRefreshToken): ...
async logoutAll(userId, jti): ...
```

**`src/services/loyalty.service.ts`:**
```typescript
calculateTier(bookingCount: number): LoyaltyTier {
  if (bookingCount >= 15) return LoyaltyTier.PLATINUM;
  if (bookingCount >= 5)  return LoyaltyTier.GOLD;
  return LoyaltyTier.SILVER;
}

async handleBookingCompleted(userId: string): Promise<void> {
  const user = await userRepository.findById(userId);
  const newCount = user.bookingCount + 1;
  const newTier  = this.calculateTier(newCount);
  await userRepository.updateById(userId, { bookingCount: newCount, loyaltyTier: newTier });
  if (newTier !== user.loyaltyTier) {
    await outboxRepository.create('USER_LOYALTY_UPDATED', {
      userId, previousTier: user.loyaltyTier, newTier,
    });
  }
}
```

**Validation:** Write unit tests in `tests/unit/services/`. Mock the repositories. Test every branch: wrong password, locked account, expired token, duplicate email, etc. Run `npm test`. All pass.

---

### Step 6: Controllers & Routes

**What to do:**

**`src/controllers/auth.controller.ts`:**
```typescript
// Every method follows this exact pattern:
async register(req: Request, res: Response) {
  const traceId = req.headers['x-correlation-id'] as string;
  const result = await authService.register(req.body);
  sendSuccess({ res, statusCode: 201, message: 'Registration successful. ...', data: result, traceId });
}
// Controllers have zero business logic — just: call service, send response
```

**`src/routes/auth.routes.ts`:**
```typescript
import { Router } from 'express';
import { validate } from '../middlewares/validate';
import { requireAuth } from '../middlewares/requireAuth';
import { authController } from '../controllers/auth.controller';
import * as schemas from './schemas/auth.schemas';

const router = Router();

// Public routes
router.post('/register',            validate(schemas.RegisterSchema),           authController.register);
router.post('/verify-email',        validate(schemas.VerifyEmailSchema),        authController.verifyEmail);
router.post('/resend-verification', validate(schemas.ResendVerificationSchema), authController.resendVerification);
router.post('/login',               validate(schemas.LoginSchema),              authController.login);
router.post('/refresh',             validate(schemas.RefreshTokenSchema),       authController.refresh);
router.post('/forgot-password',     validate(schemas.ForgotPasswordSchema),     authController.forgotPassword);
router.post('/reset-password',      validate(schemas.ResetPasswordSchema),      authController.resetPassword);

// Protected routes (require Gateway-injected auth headers)
router.post('/logout',              requireAuth, authController.logout);
router.post('/logout-all',          requireAuth, authController.logoutAll);
router.get('/me',                   requireAuth, authController.getProfile);
router.put('/me',                   requireAuth, validate(schemas.UpdateProfileSchema), authController.updateProfile);
router.post('/change-password',     requireAuth, validate(schemas.ChangePasswordSchema), authController.changePassword);

export { router as authRouter };
```

**`src/app.ts`:**
```typescript
// Note: no 'express-async-errors' needed — Express 5 propagates async errors natively
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import { pinoHttp } from 'pino-http';
import { authRouter } from './routes/auth.routes';
import { healthRouter } from './routes/health.routes';
import { jwksRouter } from './routes/jwks.routes';
import { globalErrorHandler } from './middlewares/errorHandler';
import { logger } from './config/logger';
import { env } from './config/env';

const app = express();

app.use(helmet());
app.use(cors({ origin: false }));     // No direct browser access — Gateway handles CORS
app.use(pinoHttp({ logger }));

// Raw body for Stripe webhooks (if added later)
app.use('/api/v1/webhooks', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '10kb' }));  // Limit request body size

// Routes
app.use('/api/v1/auth', authRouter);
app.use('/.well-known', jwksRouter);
app.use('/', healthRouter);

// Global error handler — must be last
app.use(globalErrorHandler);

export { app };
```

**`src/server.ts`:**
```typescript
import { app } from './app.js';
import { env } from './config/env.js';
import { prisma } from './config/database.js';
import { redisClient } from './config/redis.js';
import { kafkaProducer } from './config/kafka.js';
import { loadKeys } from './config/keys.js';
import { startOutboxWorker } from './events/outbox.worker.js';
import { startBookingConsumer } from './events/consumers/booking.consumer.js';
import { logger } from './config/logger.js';

async function bootstrap() {
  await loadKeys();                          // RSA keys must load before any JWT is signed
  logger.info('RSA keys loaded');

  await prisma.$connect();
  logger.info('PostgreSQL connected');

  await redisClient.ping();
  logger.info('Redis connected');

  await kafkaProducer.connect();
  logger.info('Kafka producer connected');

  startOutboxWorker();
  logger.info('Outbox worker started');

  await startBookingConsumer();
  logger.info('Booking consumer started');

  const server = app.listen(env.PORT, () => {
    logger.info({ port: env.PORT }, 'User service listening');
  });

  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Shutdown signal received');
    server.close(async () => {
      await prisma.$disconnect();
      await redisClient.quit();
      await kafkaProducer.disconnect();
      logger.info('Graceful shutdown complete');
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

bootstrap().catch(err => {
  logger.error(err, 'Bootstrap failed');
  process.exit(1);
});
```

**`src/routes/health.routes.ts`:**
```typescript
import { Router } from 'express';
import { prisma } from '../config/database.js';
import { redisClient } from '../config/redis.js';
import { kafkaProducer } from '../config/kafka.js';

const router = Router();

router.get('/health', async (_req, res) => {
  const checks: Record<string, string> = {};

  try {
    await prisma.$queryRaw`SELECT 1`;
    checks.database = 'ok';
  } catch (e) {
    checks.database = `error: ${(e as Error).message}`;
  }

  try {
    await redisClient.ping();
    checks.redis = 'ok';
  } catch (e) {
    checks.redis = `error: ${(e as Error).message}`;
  }

  try {
    // kafkaProducer.isConnected() is the lightest check — no network call
    checks.kafka = kafkaProducer ? 'ok' : 'not connected';
  } catch (e) {
    checks.kafka = `error: ${(e as Error).message}`;
  }

  const healthy = Object.values(checks).every(v => v === 'ok');
  res.status(healthy ? 200 : 503).json({
    status:    healthy ? 'healthy' : 'degraded',
    service:   'user-service',
    version:   '1.0.0',
    timestamp: new Date().toISOString(),
    checks,
  });
});

export { router as healthRouter };
```

**`src/routes/metrics.routes.ts`:**
```typescript
import { Router } from 'express';
import { register, collectDefaultMetrics } from 'prom-client';

collectDefaultMetrics({ prefix: 'user_service_' });  // CPU, memory, event loop lag

const router = Router();

router.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

export { router as metricsRouter };
```

Add `metricsRouter` to `app.ts` alongside the other routes:
```typescript
import { metricsRouter } from './routes/metrics.routes.js';
// ...
app.use('/', metricsRouter);   // GET /metrics — Prometheus scrape endpoint
```

**Validation:** `npm run dev` — server starts, logs `User service listening on port 3001`. Hit `GET http://localhost:3001/health` — returns `{ status: "healthy" }`. Hit `GET http://localhost:3001/metrics` — returns Prometheus text. Hit `POST http://localhost:3001/api/v1/auth/register` with valid body — returns `201 Created`.

---

### Step 7: Outbox Worker & Kafka Events

**`src/events/outbox.worker.ts`:**
```typescript
import { outboxRepository } from '../repositories/outbox.repository';
import { userProducer } from './producers/user.producer';
import { logger } from '../config/logger';

export function startOutboxWorker(): void {
  setInterval(async () => {
    try {
      const pending = await outboxRepository.getPending(100);
      for (const event of pending) {
        try {
          await userProducer.publish(event.eventType, event.payload);
          await outboxRepository.markPublished(event.id);
        } catch (err) {
          logger.error({ eventId: event.id, err }, 'Failed to publish outbox event');
          await outboxRepository.markFailed(event.id);
        }
      }
    } catch (err) {
      logger.error(err, 'Outbox worker error');
    }
  }, 5000);  // Poll every 5 seconds
}
```

**`src/events/producers/user.producer.ts`:**
```typescript
import { kafkaProducer } from '../../config/kafka';
import { env } from '../../config/env';
import { generateJti } from '../../utils/crypto.utils';

export const userProducer = {
  async publish(eventType: string, payload: object): Promise<void> {
    await kafkaProducer.send({
      topic: env.KAFKA_TOPIC_USER_EVENTS,
      messages: [{
        key: (payload as any).userId ?? generateJti(),
        value: JSON.stringify({
          eventId:       generateJti(),
          eventType,
          eventVersion:  '1.0',
          source:        'user-service',
          timestamp:     new Date().toISOString(),
          payload,
        }),
      }],
    });
  },
};
```

**`src/events/consumers/booking.consumer.ts`:**
```typescript
import { Kafka } from 'kafkajs';
import { env } from '../../config/env';
import { loyaltyService } from '../../services/loyalty.service';
import { logger } from '../../config/logger';

export async function startBookingConsumer(): Promise<void> {
  const kafka  = new Kafka({ clientId: env.KAFKA_CLIENT_ID, brokers: env.KAFKA_BROKERS.split(',') });
  const consumer = kafka.consumer({ groupId: env.KAFKA_CONSUMER_GROUP_ID });

  await consumer.connect();
  await consumer.subscribe({ topic: 'booking-events', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value?.toString() ?? '{}');
      if (event.eventType !== 'BOOKING_COMPLETED') return;

      try {
        await loyaltyService.handleBookingCompleted(event.payload.userId);
        logger.info({ userId: event.payload.userId }, 'Booking count updated');
      } catch (err) {
        logger.error({ err, event }, 'Failed to handle BOOKING_COMPLETED');
      }
    },
  });
}
```

**Validation:**
1. Start Kafka: `docker compose up -d kafka`
2. Start the service: `npm run dev`
3. In a second terminal, use `kafkajs` or `kcat` to produce a test event to `booking-events`
4. Watch the service logs — it should process the event and update `booking_count` in DB
5. After updating booking_count to 5: check Prisma Studio — `loyalty_tier` should change to `GOLD`
6. Check `outbox_events` table — a `USER_LOYALTY_UPDATED` row should appear with status `PUBLISHED`

---

### Step 8: JWKS Endpoint

> `src/config/keys.ts` was already created in Step 3. This step wires up the public-facing route that exposes the public key to other services.

**`src/routes/jwks.routes.ts`:**
```typescript
import { Router } from 'express';
import { getPublicJwk } from '../config/keys';

const router = Router();

router.get('/jwks.json', (req, res) => {
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.json({ keys: [getPublicJwk()] });
});

export { router as jwksRouter };
```

Call `loadKeys()` in `server.ts` before starting the HTTP listener.

**Validation:** `GET http://localhost:3001/.well-known/jwks.json` returns a valid JWK object with `kty: "RSA"`, `alg: "RS256"`, `kid: "skyhub-key-v1"`.

---

### Step 9: Integration Testing

Write integration tests that test the full HTTP flow against a real test database:

**`tests/integration/auth.register.test.ts`:**
```typescript
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import { app } from '../../src/app';

describe('POST /api/v1/auth/register', () => {
  it('returns 201 with user data for valid input', async () => {
    const res = await request(app).post('/api/v1/auth/register').send({
      name: 'Test User', email: 'test@example.com', password: 'StrongPass1!',
    });
    expect(res.status).toBe(201);
    expect(res.body.success).toBe(true);
    expect(res.body.data.email).toBe('test@example.com');
    expect(res.body.data).not.toHaveProperty('passwordHash');
  });

  it('returns 409 when email already exists', async () => {
    const body = { name: 'Test', email: 'duplicate@example.com', password: 'StrongPass1!' };
    await request(app).post('/api/v1/auth/register').send(body);
    const res = await request(app).post('/api/v1/auth/register').send(body);
    expect(res.status).toBe(409);
    expect(res.body.error.code).toBe('CONFLICT');
  });

  it('returns 400 for weak password', async () => {
    const res = await request(app).post('/api/v1/auth/register').send({
      name: 'Test', email: 'weak@example.com', password: 'weak',
    });
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('VALIDATION_ERROR');
  });
});
```

**Validation checklist before marking a step complete:**
- `POST /register` → 201, no passwordHash in response
- `POST /register` with same email → 409
- `POST /login` before email verification → 401 EMAIL_NOT_VERIFIED
- `POST /verify-email` with valid token → 200
- `POST /login` after verification → 200 with access + refresh tokens
- `GET /me` with valid Bearer token → 200 with profile
- `POST /refresh` → 200 with NEW tokens (old refresh token no longer works)
- `POST /logout` → 200, old access token now returns 401 at Gateway blacklist check
- 5× wrong password → account locked for 30 min → 423 with `lockExpiresAt`
- `POST /forgot-password` with non-existent email → still 200 (no enumeration)
- `POST /reset-password` with valid token → 200, old password no longer works

---

## 11. Testing Strategy

### Unit Tests (Pure logic — no DB, no network)

Test services by mocking repositories:

```typescript
// tests/unit/services/auth.service.test.ts
import { vi, describe, it, expect } from 'vitest';

vi.mock('../../src/repositories/user.repository', () => ({
  userRepository: {
    findByEmail: vi.fn(),
    create: vi.fn(),
  }
}));

describe('authService.register', () => {
  it('throws 409 when email already registered', async () => {
    userRepository.findByEmail.mockResolvedValue({ id: '123' });
    await expect(authService.register({ name: 'A', email: 'exists@test.com', password: 'X' }))
      .rejects.toMatchObject({ code: 'CONFLICT' });
  });
});
```

### Integration Tests (Full HTTP — real test DB)

Use a separate `skyhub_user_test_db` database. Reset tables before each test suite:
```typescript
beforeEach(async () => {
  await prisma.refreshToken.deleteMany();
  await prisma.outboxEvent.deleteMany();
  await prisma.user.deleteMany();
});
```

### What to Test

| Area | Test Cases |
|---|---|
| Registration | Valid input, duplicate email, weak password, missing fields |
| Email verification | Valid token, expired token, already verified |
| Login | Correct credentials, wrong password, unverified account, locked account, non-existent email |
| Account lockout | 5 failed attempts → locked, locked account returns correct `secondsRemaining` |
| Token refresh | Valid rotation, expired refresh token, reuse after rotation |
| Logout | Single session, all sessions, blacklist check |
| Password reset | Valid flow, expired token, password same as old |
| Profile update | Valid name, name too short, unauthenticated |
| Loyalty tiers | SILVER→GOLD at 5 bookings, GOLD→PLATINUM at 15 |

---

## Quick Reference: Build Checklist

```
Step 1  ✓  Package.json, tsconfig.json, Prisma init, .env created
Step 2  ✓  Migration run, tables created, seed data inserted
Step 3  ✓  Config singletons (DB, Redis, Kafka, logger, keys), utilities, middlewares, email service, JWT types
Step 4  ✓  User, Token, Outbox repositories implemented and tested
Step 5  ✓  AuthService, TokenService, LoyaltyService implemented and unit tested
Step 6  ✓  Controllers, Routes, app.ts, server.ts — service boots and /health returns 200
Step 7  ✓  OutboxWorker running, Kafka producer publishing, Booking consumer processing
Step 8  ✓  JWKS endpoint returning public key
Step 9  ✓  Integration tests pass for all 10 validation checklist items
```

Once all 9 steps pass their validation, Phase 1 (User Service) is complete and production-ready. Proceed to Phase 2: Flight Service.

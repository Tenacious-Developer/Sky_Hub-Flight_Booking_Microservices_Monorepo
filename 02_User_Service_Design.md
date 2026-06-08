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

## 2.1 Enterprise-Grade Feature Enhancements (IAM Standards)

To ensure this service matches professional production-grade systems (like **Auth0, Keycloak, and Clerk**), the core auth capabilities are reinforced with the following industry-standard enhancements.

### 1. Granular Permissions & Scopes inside JWT Claims
Instead of hardcoding standard roles (e.g. `CUSTOMER` or `FLIGHT_ADMIN`) inside downstream microservices, the User Service translates dynamic RBAC relationships at login and injects a dedicated `permissions` string array (scopes) directly into the Access Token claims:

```json
{
  "sub": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
  "jti": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "role": "FLIGHT_ADMIN",
  "permissions": [
    "flights:read",
    "flights:create",
    "flights:delete",
    "bookings:read"
  ],
  "iat": 1782500000,
  "exp": 1782500900
}
```
*   **Decoupled Verification**: Downstream microservices check if the token possesses the specific permission (e.g., `'flights:create'`), completely decoupling route security logic from central user administration.

### 2. 6-Digit Cryptographic Numeric OTPs
To ensure seamless native iOS/Android mobile compatibility and reduce verification friction, verification links are replaced with **cryptographically secure 6-digit numeric One-Time Passwords (OTPs)**:
1.  Generate raw code via `crypto.randomInt(100000, 999999).toString()`.
2.  Encrypt it using **SHA-256** and store the hash with a strict 10-minute expiration window on the user record (`email_verify_token`).
3.  The client inputs the raw 6-digit code which is hashed and matched inside `/api/v1/auth/verify-otp`.

### 3. Device-Aware Active Session Control
Users can view, manage, and selectively revoke their active logins from other devices (avoiding full-device lockouts).

*   **`GET /api/v1/auth/sessions` (Auth Required)**: Parses the request's `User-Agent` string into human-readable device and browser metrics, fetching all active sessions from the `refresh_tokens` database table.
*   **`DELETE /api/v1/auth/sessions/:sessionId` (Auth Required)**: Instantly deletes the specified refresh token row, forcing the targeted device to log out without disrupting the user's current session.

### 4. Time-Based Authenticator MFA (TOTP)
Optional security reinforcement using authenticator tools (Google Authenticator, Microsoft Authenticator):
1.  **Enable MFA**: Generate a cryptographically random Base32 secret key and output a standard provisioning URL: `otpauth://totp/SkyHub:user@email.com?secret=SECRETKEY&issuer=SkyHub`.
2.  **Verify Setup**: Validate the user's initial code entry using the `otplib` library. Set `mfa_enabled = true` on the database record.
3.  **Step-Up Auth**: If `mfa_enabled` is active during login, the service returns a status `MFA_REQUIRED` alongside a temporary token. The final Access/Refresh tokens are only generated once the client submits their authenticator OTP to `/api/v1/auth/mfa/verify`.

### 5. Immutable Security Audit Logging
Critical security operations compile structured history logs into an analytical pipeline (or Kafka stream `security-audit-events`) to ensure full compliance auditing:

```json
{
  "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
  "action": "USER_LOCKED",
  "ipAddress": "192.168.1.99",
  "device": "Firefox / Ubuntu Linux",
  "metadata": {
    "reason": "5 consecutive failed login attempts",
    "lockExpiresAt": "2026-05-30T16:00:00Z"
  },
  "timestamp": "2026-05-30T15:30:00Z"
}
```

---

## 3. Database Design & Prisma Schema

To align with modern industry-standard designs for **Identity & Access Management (IAM)** and to ensure strict security, this database architecture separates **Core Authentication Credentials**, **Public Profile metadata**, **Active Sessions**, **Security Audit Logs**, and **Granular Authorization (Dynamic RBAC)** into distinct decoupled tables.

### 3.1 Entity-Relationship Diagram

```
                              ┌────────────────────────┐
                              │         USERS          │
                              │ (Core Auth Credentials)│
                              ├────────────────────────┤
                              │ id (UUID) [PK]         │
                              │ email (VARCHAR) [UQ]   │
                              │ password_hash (VARCHAR)│
                              │ is_active (BOOLEAN)    │
                              │ failed_attempts (INT)  │
                              │ locked_until (TIMESTAMPTZ)
                              │ mfa_enabled (BOOLEAN)  │
                              │ mfa_secret (VARCHAR)   │
                              └───────────┬────────────┘
                                          │ 1
                                          │
                  ┌───────────────────────┼───────────────────────┐
                  │ 1                     │ 1                     │ 1
                  ▼                       ▼                       ▼
      ┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
      │     USER_PROFILES     │  │    REFRESH_TOKENS     │  │      AUDIT_LOGS       │
      ├───────────────────────┤  ├───────────────────────┤  ├───────────────────────┤
      │ user_id (UUID)[PK, FK]│  │ id (UUID) [PK]         │  │ id (UUID) [PK]         │
      │ full_name (VARCHAR)   │  │ user_id (UUID) [FK]    │  │ user_id (UUID) [FK]    │
      │ loyalty_tier (ENUM)   │  │ token_hash (VARCHAR) UQ│  │ action (VARCHAR)      │
      │ booking_count (INT)   │  │ expires_at (TIMESTAMPTZ)│  │ timestamp (TIMESTAMPTZ)│
      └───────────────────────┘  └───────────────────────┘  └───────────────────────┘

                           - - - - - - - - - - - - - - - - - - - - - - - - - - - -
                                                     RBAC MODULE
                        ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
                        │    USERS     │ 1  N │  USER_ROLES  │ N  1 │    ROLES     │
                        │ (Auth Table) ├─────►│ (Join Table) ◄──────┤ (Admin/Cust) │
                        └──────────────┘      └──────────────┘      └──────┬───────┘
                                                                           │ 1
                                                                           │
                                                                           ▼ N
                                                                    ┌──────────────┐
                                                                    │  ROLE_PERMS  │
                                                                    │ (Join Table) │
                                                                    └──────▲───────┘
                                                                           │ N
                                                                           │ 1
                                                                    ┌──────┴───────┐
                                                                    │ PERMISSIONS  │
                                                                    │(read:flights)│
                                                                    └──────────────┘
```

---

### 3.2 Column-by-Column Justification

#### `users` (Core Identity & Security Credentials)
This table acts as the vault. It only handles identity verification, multi-factor settings, security lockout metrics, and account status/lifecycles.

| Column | Type | Why This Design Choice |
| :--- | :--- | :--- |
| `id` | UUID | Globally unique, safe from business intelligence leaks (auto-increment integers leak scale). |
| `email` | VARCHAR(255) | B-Tree indexed and unique. Used as the unique login handle. |
| `password_hash` | VARCHAR(255) | Holds the highly secure Bcrypt hash (rounds=12). Never loaded during profile requests. |
| `is_active` | BOOLEAN | Allows administrative soft deactivation (e.g. banning) without wiping audit histories. |
| `failed_login_attempts` | INT | Lockout tracker. Incremented on wrong passwords, reset on success. |
| `locked_until` | TIMESTAMPTZ | Absolute lock expiration time. The auth pipeline verifies `locked_until > NOW()`. |
| `mfa_enabled` | BOOLEAN | Indicates if the user has completed authenticator TOTP setup. |
| `mfa_secret` | VARCHAR(255) | Stores the cryptographically encrypted Base32 MFA secret key. |
| `mfa_backup_codes` | JSON | Holds hashed MFA backup recovery codes to prevent lockout if user loses device. |
| `email_verify_token` | VARCHAR(255) | SHA-256 hash of the 6-digit OTP. B-Tree indexed but not globally unique to prevent unique constraint conflicts (due to low-entropy OTP space collisions). |
| `reset_token` | VARCHAR(255) | SHA-256 hash of the password recovery 6-digit OTP. B-Tree indexed but not unique for OTP collision safety. |
| `deleted_at` | TIMESTAMPTZ | Enables soft-deletion for GDPR compliance while maintaining foreign-key data integrity. |

#### `user_profiles` (Public Details & Domain metadata)
Separating profile data ensures credentials cannot be accidentally leaked during standard metadata fetches. `user_id` acts as the primary key.

| Column | Type | Why This Design Choice |
| :--- | :--- | :--- |
| `user_id` | UUID (PK, FK) | Unique primary key mapping back 1-to-1 to the `users` table. Saves space and index overhead compared to redundant UUID keys. Cascade deletes. |
| `full_name` | VARCHAR(100) | Standard username container, trimmed. |
| `loyalty_tier` | ENUM (`LoyaltyTier`) | Holds `'SILVER'`, `'GOLD'`, or `'PLATINUM'`. Default `'SILVER'`. Enforced at DB level to prevent invalid states. |
| `booking_count` | INT | Tracked by Kafka event listener. Promotes users once threshold is crossed. |

#### `roles` & `permissions` (Decoupled RBAC Authorization)
Enabling multi-role assignment and dynamic run-time capabilities without modifying database enums or core codebases.

| Table | Column | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `roles` | `name` | VARCHAR (UQ) | e.g. `'CUSTOMER'`, `'FLIGHT_ADMIN'`, `'SUPER_ADMIN'`. |
| `permissions` | `name` | VARCHAR (UQ) | e.g. `'flights:create'`, `'users:ban'`. Allows granular check logic. |
| `user_roles` | `(user_id, role_id)` | UUID (Composite PK) | Many-to-many lookup table connecting users to multiple roles. Index added on `role_id` for inverse queries. |
| `role_permissions` | `(role_id, perm_id)` | UUID (Composite PK) | Many-to-many lookup table mapping rights to active roles. Index added on `permission_id` for inverse queries. |

#### `audit_logs` (Analytical Security Compliance Logging)
Allows administrators to audit security operations natively without performing expensive log aggregation scans.

| Column | Type | Purpose |
| :--- | :--- | :--- |
| `id` | UUID (PK) | Globally unique log identifier. |
| `user_id` | UUID (FK) | Maps back to the affected `User` (SetNull on delete). |
| `action` | VARCHAR(100) | Audit action e.g. `'USER_LOCKED'`, `'PASSWORD_CHANGED'`, `'MFA_ENABLED'`. |
| `ip_address` | VARCHAR(45) | Captures client IP (supports IPv4/IPv6). |
| `device` | VARCHAR(255) | Stores client's parsed user-agent details. |
| `metadata` | JSON | Stores structured operational contexts (reasons, parameters). |
| `timestamp` | TIMESTAMPTZ | Time of audit execution. |

---

### 3.3 Complete Production-Grade Prisma Schema

**File: `services/user-service/prisma/schema.prisma`**

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}

// ─── 1. CORE AUTHENTICATION (The Credential Vault) ───────────────────────────
model User {
  id                   String        @id @default(uuid())
  email                String        @unique
  passwordHash         String        @map("password_hash")
  isActive             Boolean       @default(true) @map("is_active")
  
  // Security / Lockout properties
  failedLoginAttempts  Int           @default(0) @map("failed_login_attempts")
  lockedUntil          DateTime?     @map("locked_until")
  lastLoginAt          DateTime?     @map("last_login_at")
  
  // MFA (TOTP) Properties
  mfaEnabled           Boolean       @default(false) @map("mfa_enabled")
  mfaSecret            String?       @map("mfa_secret")
  mfaBackupCodes       Json?         @map("mfa_backup_codes")
  
  // Verification & Reset (Removed unique constraint on low-entropy tokens to prevent OTP collisions)
  emailVerified        Boolean       @default(false) @map("email_verified")
  emailVerifyToken     String?       @map("email_verify_token") // SHA-256 hash of OTP
  emailVerifyExpiresAt DateTime?     @map("email_verify_expires_at")
  resetToken           String?       @map("reset_token") // SHA-256 hash of OTP
  resetExpiresAt       DateTime?     @map("reset_expires_at")

  // Audit and lifecycle
  createdAt            DateTime      @default(now()) @map("created_at")
  updatedAt            DateTime      @updatedAt @map("updated_at")
  deletedAt            DateTime?     @map("deleted_at")

  // Relations
  profile              UserProfile?
  refreshTokens        RefreshToken[]
  userRoles            UserRole[]
  auditLogs            AuditLog[]

  @@index([email])
  @@index([emailVerifyToken])
  @@index([resetToken])
  @@map("users")
}

// ─── 2. USER PROFILE (Optimized 1-to-1 relationship using user_id as PK) ─────
model UserProfile {
  userId       String      @id @map("user_id")
  fullName     String      @map("full_name")
  
  // Loyalty Domain
  loyaltyTier  LoyaltyTier @default(SILVER) @map("loyalty_tier")
  bookingCount Int         @default(0) @map("booking_count")

  // Foreign Key constraints
  user         User        @relation(fields: [userId], references: [id], onDelete: Cascade)

  @@map("user_profiles")
}

enum LoyaltyTier {
  SILVER
  GOLD
  PLATINUM
}

// ─── 3. DYNAMIC RBAC (Roles & Granular Permissions) ─────────────────────────
model Role {
  id              String           @id @default(uuid())
  name            String           @unique // e.g. "CUSTOMER", "FLIGHT_ADMIN", "SUPER_ADMIN"
  description     String?
  
  userRoles       UserRole[]
  rolePermissions RolePermission[]

  @@map("roles")
}

model Permission {
  id              String           @id @default(uuid())
  name            String           @unique // e.g. "flights:create", "flights:delete", "users:ban"
  description     String?

  rolePermissions RolePermission[]

  @@map("permissions")
}

// Many-to-Many Join Table: Users to Roles
model UserRole {
  userId      String       @map("user_id")
  roleId      String       @map("role_id")

  user        User         @relation(fields: [userId], references: [id], onDelete: Cascade)
  role        Role         @relation(fields: [roleId], references: [id], onDelete: Cascade)

  @@id([userId, roleId]) // Composite Primary Key
  @@index([roleId])      // Speeds up searching users by role
  @@map("user_roles")
}

// Many-to-Many Join Table: Roles to Permissions
model RolePermission {
  roleId       String      @map("role_id")
  permissionId String      @map("permission_id")

  role         Role        @relation(fields: [roleId], references: [id], onDelete: Cascade)
  permission   Permission  @relation(fields: [permissionId], references: [id], onDelete: Cascade)

  @@id([roleId, permissionId]) // Composite Primary Key
  @@index([permissionId])      // Speeds up searching roles by permission
  @@map("role_permissions")
}

// ─── 4. SECURITY TOKENS & OUTBOX EVENTS ─────────────────────────────────────
model RefreshToken {
  id         String   @id @default(uuid()) // Session ID
  userId     String   @map("user_id")
  tokenHash  String   @unique @map("token_hash") // SHA-256 hash of rotated token
  deviceInfo String?  @map("device_info")
  ipAddress  String?  @map("ip_address")
  expiresAt  DateTime @map("expires_at")
  createdAt  DateTime @default(now()) @map("created_at")

  user       User     @relation(fields: [userId], references: [id], onDelete: Cascade)

  @@index([userId])
  @@map("refresh_tokens")
}

model OutboxEvent {
  id          String   @id @default(uuid())
  eventType   String   @map("event_type")
  payload     Json
  status      String   @default("PENDING") // PENDING, PUBLISHED, FAILED
  createdAt   DateTime @default(now()) @map("created_at")
  publishedAt DateTime? @map("published_at")

  @@index([status, createdAt])
  @@map("outbox_events")
}

// ─── 5. SECURITY AUDIT LOGS ─────────────────────────────────────────────────
model AuditLog {
  id        String   @id @default(uuid())
  userId    String?  @map("user_id")
  action    String   @map("action")
  ipAddress String?  @map("ip_address")
  device    String?  @map("device")
  metadata  Json?    @map("metadata")
  timestamp DateTime @default(now()) @map("timestamp")

  user      User?    @relation(fields: [userId], references: [id], onDelete: SetNull)

  @@index([userId])
  @@index([action])
  @@index([timestamp])
  @@map("audit_logs")
}
```

### 3.4 Database Indexes Summary

| Table | Index | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `users` | `email` | B-Tree UNIQUE | Exact match credential lookups during authentication. |
| `users` | `email_verify_token` | B-Tree UNIQUE | One-way hash verification lookup. |
| `users` | `reset_token` | B-Tree UNIQUE | Password recovery token validation. |
| `user_profiles` | `user_id` | B-Tree UNIQUE | Dynamic 1-to-1 fetching for metadata. |
| `roles` | `name` | B-Tree UNIQUE | Role checking constraint. |
| `permissions` | `name` | B-Tree UNIQUE | Permission checking constraint. |
| `refresh_tokens` | `token_hash` | B-Tree UNIQUE | O(1) matching on `/refresh` session validations. |
| `refresh_tokens` | `user_id` | B-Tree | Locates all sessions for single/global logs. |
| `outbox_events` | `(status, created_at)` | Composite B-Tree | High-speed polling query indexing. |

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

**JWT payload (minimal — no PII, granular scopes included):**
```json
{
  "sub":         "7b58c281-a5bf-4050-a922-a72a1cd40a92",
  "role":        "CUSTOMER",
  "loyaltyTier": "SILVER",
  "permissions": [
    "flights:read",
    "bookings:read",
    "bookings:create"
  ],
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

All endpoints are prefixed with `/api/v1` at the Gateway level. Internally, the User Service listens on `/api/v1/auth` and `/api/v1/admin` routes.

### Standard Response Envelope

Every response, success or error, uses this uniform JSON shape:

```typescript
// Success Response Envelope
{
  success: true,
  message: string,
  data: object | array | null,
  traceId: string         // Unique correlation ID
}

// Error Response Envelope (Ultimate Industry-Standard)
{
  success: false,
  error: {
    statusCode: number,   // Numeric HTTP Status Code (e.g. 409, 400, 401)
    name: string,         // Machine-readable error code (e.g. 'CONFLICT', 'VALIDATION_ERROR')
    message: string,      // Human-readable message
    details?: Array<{ field: string, message: string }> // Optional field validations
  },
  traceId: string
}
```

---

### 5.1 Standard Authentication & Profile Endpoints

#### Endpoint 1: POST /api/v1/auth/register
*   **Auth required**: No
*   **Request Body**:
    ```json
    {
      "name": "John Doe",
      "email": "john.doe@example.com",
      "password": "SecurePassword1!"
    }
    ```
*   **Behavior**: Creates a deactivated `User` and `UserProfile` in a single transaction. Generates a secure, cryptographically random **6-digit numeric OTP** (`email_verify_token`), hashes it using SHA-256, saves it, and emits a `USER_REGISTERED` event to Kafka via the outbox pattern.
*   **Success Response — 201 Created**:
    ```json
    {
      "success": true,
      "message": "Registration successful. Please enter the 6-digit OTP sent to your email.",
      "data": {
        "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
        "email": "john.doe@example.com",
        "name": "John Doe",
        "emailVerified": false
      },
      "traceId": "tr-ka7e12mx-9a7x12"
    }
    ```
*   **Error Response — 400 Bad Request (Validation Failure)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 400,
        "name": "VALIDATION_ERROR",
        "message": "Validation failed",
        "details": [
          { "field": "email", "message": "Invalid email format" },
          { "field": "password", "message": "Password must be at least 8 characters" }
        ]
      },
      "traceId": "tr-ka7e12mx-9a7x12"
    }
    ```
*   **Error Response — 409 Conflict (Duplicate Email)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 409,
        "name": "CONFLICT",
        "message": "An account with this email address is already registered."
      },
      "traceId": "tr-ka7e12mx-9a7x12"
    }
    ```

#### Endpoint 2: POST /api/v1/auth/verify-email
*   **Auth required**: No
*   **Request Body**:
    ```json
    {
      "email": "john.doe@example.com",
      "code": "482910"
    }
    ```
*   **Behavior**: Computes SHA-256 of the code, verifies it matches the stored `email_verify_token` for the user, checks that token has not expired, and updates the user's status to `emailVerified = true`.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Email verified successfully. You can now log in.",
      "data": null,
      "traceId": "tr-ab7x82ld-291a"
    }
    ```
*   **Error Response — 400 Bad Request (Invalid/Expired OTP)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 400,
        "name": "INVALID_VERIFY_CODE",
        "message": "The verification code is incorrect or has expired. Please request a new code."
      },
      "traceId": "tr-ab7x82ld-291a"
    }
    ```

#### Endpoint 3: POST /api/v1/auth/resend-verification
*   **Auth required**: No
*   **Request Body**:
    ```json
    { "email": "john.doe@example.com" }
    ```
*   **Behavior**: Generates a new 6-digit OTP and resends it. Rate-limited to 1 request per 2 minutes per account to prevent email bombing. Returns a decoy message if the email does not exist.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "If this account is unverified, a new 6-digit code has been dispatched.",
      "data": null,
      "traceId": "tr-mn8x71ba-8172"
    }
    ```
*   **Error Response — 429 Too Many Requests (Rate Limited)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 429,
        "name": "RATE_LIMIT_EXCEEDED",
        "message": "Too many requests. Please wait 2 minutes before requesting another verification code."
      },
      "traceId": "tr-mn8x71ba-8172"
    }
    ```

#### Endpoint 4: POST /api/v1/auth/login
*   **Auth required**: No
*   **Request Body**:
    ```json
    {
      "email": "john.doe@example.com",
      "password": "SecurePassword1!"
    }
    ```
*   **Behavior**: 
    1. Looks up user. If locked (`locked_until > NOW()`), returns `423 Locked`.
    2. Runs timing-safe Bcrypt password comparison. 
    3. If failed, increments lockout counters (locks at 5).
    4. If successful, checks if user has `mfa_enabled = true`. 
       - If **MFA Active**: Returns a short-lived temporary ticket token and sets `mfaRequired = true`.
       - If **No MFA**: Signs the RS256 Access Token (attaching dynamic scopes/permissions in claims) and writes a new SHA-256 refresh token hash to the DB.
*   **Success Response (MFA Inactive) — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Login successful.",
      "data": {
        "mfaRequired": false,
        "user": {
          "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
          "email": "john.doe@example.com",
          "fullName": "John Doe",
          "loyaltyTier": "SILVER"
        },
        "tokens": {
          "accessToken": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
          "refreshToken": "a3f2b1c4...128-hex-chars...",
          "expiresAt": 900
        }
      },
      "traceId": "tr-kl89x12a-381a"
    }
    ```
*   **Success Response (MFA Required) — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Step-up Multi-Factor Authentication required.",
      "data": {
        "mfaRequired": true,
        "mfaTicket": "temp_mfa_token_xyz123"
      },
      "traceId": "tr-kl89x12a-381a"
    }
    ```
*   **Error Response — 401 Unauthorized (Invalid Credentials)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "UNAUTHORIZED",
        "message": "Invalid email address or password. Please try again."
      },
      "traceId": "tr-kl89x12a-381a"
    }
    ```
*   **Error Response — 401 Unauthorized (Email Unverified)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "EMAIL_NOT_VERIFIED",
        "message": "Your email address is not verified. Please verify your email before logging in."
      },
      "traceId": "tr-kl89x12a-381a"
    }
    ```
*   **Error Response — 423 Locked (Brute Force Protection)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 423,
        "name": "ACCOUNT_LOCKED",
        "message": "Account locked due to 5 consecutive failed login attempts. Please try again in 30 minutes.",
        "details": [
          { "field": "lockedUntil", "message": "2026-05-30T16:00:00.000Z" },
          { "field": "secondsRemaining", "message": "1800" }
        ]
      },
      "traceId": "tr-kl89x12a-381a"
    }
    ```

#### Endpoint 5: POST /api/v1/auth/refresh
*   **Auth required**: No
*   **Request Body**:
    ```json
    { "refreshToken": "a3f2b1c4...128-hex-chars..." }
    ```
*   **Behavior**: Looks up the SHA-256 hash of the token. If found, deletes it, signs a fresh access token, generates a new refresh token, and inserts the new hash (Token Rotation).
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Session token rotated successfully.",
      "data": {
        "accessToken": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
        "refreshToken": "b4c3d2e1...new-128-hex-chars..."
      },
      "traceId": "tr-rf82x91b-82ba"
    }
    ```
*   **Error Response — 401 Unauthorized (Invalid/Stolen Session)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "INVALID_REFRESH_TOKEN",
        "message": "The session has expired, been terminated, or is invalid. Please log in again."
      },
      "traceId": "tr-rf82x91b-82ba"
    }
    ```

#### Endpoint 6: POST /api/v1/auth/logout
*   **Auth required**: Yes (Bearer Access Token)
*   **Request Body**:
    ```json
    { "refreshToken": "a3f2b1c4..." }
    ```
*   **Behavior**: Deletes the specific refresh token hash from the database. Writes the Access Token's unique `jti` to the Redis blacklist with a TTL matching its remaining time to prevent reuse.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Logged out successfully.",
      "data": null,
      "traceId": "tr-lo7x81ca-92ba"
    }
    ```

#### Endpoint 7: POST /api/v1/auth/logout-all
*   **Auth required**: Yes (Bearer Access Token)
*   **Behavior**: Deletes ALL active refresh tokens for this user from the database. Blacklists current `jti` in Redis.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Logged out from all session devices successfully.",
      "data": null,
      "traceId": "tr-lo9y12ca-1029"
    }
    ```

#### Endpoint 8: GET /api/v1/auth/me
*   **Auth required**: Yes (Bearer Access Token)
*   **Behavior**: Fetches the profile from the `user_profiles` table. Strictly isolates data; **never** queries or exposes credential hashes or security parameters.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Profile retrieved successfully.",
      "data": {
        "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
        "email": "john.doe@example.com",
        "profile": {
          "fullName": "John Doe",
          "loyaltyTier": "SILVER",
          "bookingCount": 0
        }
      },
      "traceId": "tr-me8a21kb-921a"
    }
    ```
*   **Error Response — 401 Unauthorized (Expired JWT)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "TOKEN_EXPIRED",
        "message": "Authentication token has expired. Please refresh your session."
      },
      "traceId": "tr-me8a21kb-921a"
    }
    ```

#### Endpoint 9: PUT /api/v1/auth/me
*   **Auth required**: Yes (Bearer Access Token)
*   **Request Body**:
    ```json
    { "fullName": "John Smith" }
    ```
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Profile updated successfully.",
      "data": {
        "fullName": "John Smith"
      },
      "traceId": "tr-up82x1ab-812a"
    }
    ```

#### Endpoint 10: POST /api/v1/auth/change-password
*   **Auth required**: Yes (Bearer Access Token)
*   **Request Body**:
    ```json
    {
      "currentPassword": "SecurePassword1!",
      "newPassword": "EvenMoreSecure2@"
    }
    ```
*   **Behavior**: Checks new password meets criteria, runs `bcrypt.compare` against the old password. On match, hashes the new password and forces a logout of all other devices (`logout-all` logic).
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Password changed successfully. All other devices have been signed out.",
      "data": null,
      "traceId": "tr-cp89x21b-89ba"
    }
    ```
*   **Error Response — 400 Bad Request (Same Password)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 400,
        "name": "BUSINESS_RULE_VIOLATION",
        "message": "New password cannot be identical to your current password."
      },
      "traceId": "tr-cp89x21b-89ba"
    }
    ```
*   **Error Response — 401 Unauthorized (Wrong Current Password)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "UNAUTHORIZED",
        "message": "The current password provided is incorrect."
      },
      "traceId": "tr-cp89x21b-89ba"
    }
    ```

#### Endpoint 11: POST /api/v1/auth/forgot-password
*   **Auth required**: No
*   **Request Body**:
    ```json
    { "email": "john.doe@example.com" }
    ```
*   **Behavior**: Generates a 6-digit password recovery OTP, hashes it, saves it to `reset_token`, and emails it to the user. Always returns `200` to prevent user enumeration.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "If an account with that email exists, a password reset code has been sent.",
      "data": null,
      "traceId": "tr-fp82x91a-810a"
    }
    ```

#### Endpoint 12: POST /api/v1/auth/reset-password
*   **Auth required**: No
*   **Request Body**:
    ```json
    {
      "email": "john.doe@example.com",
      "code": "812903",
      "newPassword": "SuperNewPassword3#"
    }
    ```
*   **Behavior**: Computes SHA-256 of the recovery code, matches it, verifies expiration, and updates the user's password hash in the database.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Password reset successfully. You can now log in.",
      "data": null,
      "traceId": "tr-rp291xlb-918a"
    }
    ```
*   **Error Response — 400 Bad Request (Invalid OTP)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 400,
        "name": "INVALID_RESET_TOKEN",
        "message": "The password reset code is incorrect, already used, or has expired."
      },
      "traceId": "tr-rp291xlb-918a"
    }
    ```

---

### 5.2 Device-Aware Session Management Endpoints (Auth Required)

#### Endpoint 13: GET /api/v1/auth/sessions
*   **Behavior**: Reads all active `RefreshToken` entries for this user. Uses `User-Agent` headers parsed cleanly into device/browser metadata.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Active sessions retrieved successfully.",
      "data": [
        {
          "sessionId": "4b91-a20c-ef92",
          "device": "Firefox / Ubuntu Linux",
          "ipAddress": "192.168.1.102",
          "isCurrent": true,
          "createdAt": "2026-05-30T15:30:00.000Z"
        },
        {
          "sessionId": "9a38-c208-d204",
          "device": "Safari / iPhone 15 Pro",
          "ipAddress": "172.56.21.9",
          "isCurrent": false,
          "createdAt": "2026-05-28T10:15:00.000Z"
        }
      ],
      "traceId": "tr-se189kab-9182"
    }
    ```

#### Endpoint 14: DELETE /api/v1/auth/sessions/:sessionId
*   **Behavior**: Deletes the specified refresh token session by ID. If successful, that device is forced to log out immediately on its next refresh attempt.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Session terminated successfully.",
      "data": null,
      "traceId": "tr-sed291ab-812a"
    }
    ```
*   **Error Response — 403 Forbidden (Unauthorized Revocation)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 403,
        "name": "FORBIDDEN",
        "message": "You do not have permission to terminate this session."
      },
      "traceId": "tr-sed291ab-812a"
    }
    ```
*   **Error Response — 404 Not Found (Invalid Session ID)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 404,
        "name": "NOT_FOUND",
        "message": "The session ID requested was already terminated or does not exist."
      },
      "traceId": "tr-sed291ab-812a"
    }
    ```

---

### 5.3 Step-Up Multi-Factor Authentication Endpoints (Auth Required)

#### Endpoint 15: POST /api/v1/auth/mfa/enable
*   **Behavior**: Generates a standard Base32 secret key and creates a standard QR code URL.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "MFA configuration initialized. Scan the QR code with your authenticator app.",
      "data": {
        "secret": "JBSWY3DPEHPK3PXP",
        "qrCodeUrl": "otpauth://totp/SkyHub:john.doe@example.com?secret=JBSWY3DPEHPK3PXP&issuer=SkyHub"
      },
      "traceId": "tr-mfa8x21a-98ba"
    }
    ```

#### Endpoint 16: POST /api/v1/auth/mfa/verify
*   **Request Body**:
    ```json
    { "code": "382910" }
    ```
*   **Behavior**: Validates the 6-digit TOTP token using the stored secret key. Once confirmed, permanently updates user settings to `mfa_enabled = true`.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "MFA enabled and activated successfully.",
      "data": null,
      "traceId": "tr-mfav82ab-81ba"
    }
    ```
*   **Error Response — 400 Bad Request (Invalid Verification Code)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 400,
        "name": "BUSINESS_RULE_VIOLATION",
        "message": "The authenticator code is incorrect. Verification failed."
      },
      "traceId": "tr-mfav82ab-81ba"
    }
    ```

#### Endpoint 17: POST /api/v1/auth/mfa/login-verify
*   **Request Body**:
    ```json
    {
      "mfaTicket": "temp_mfa_token_xyz123",
      "code": "482910"
    }
    ```
*   **Behavior**: Validates the step-up login token. If the TOTP code matches the user's authenticator secret, issues the permanent Access and Refresh tokens.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Multi-factor verification successful. Welcome back.",
      "data": {
        "tokens": {
          "accessToken": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
          "refreshToken": "a3f2b1c4..."
        }
      },
      "traceId": "tr-mfal82ab-289a"
    }
    ```
*   **Error Response — 401 Unauthorized (Invalid Step-up Ticket)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "UNAUTHORIZED",
        "message": "The step-up login ticket is invalid, expired, or compromised. Please restart login."
      },
      "traceId": "tr-mfal82ab-289a"
    }
    ```
*   **Error Response — 401 Unauthorized (Invalid TOTP Code)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 401,
        "name": "UNAUTHORIZED",
        "message": "The authenticator code is incorrect. Access denied."
      },
      "traceId": "tr-mfal82ab-289a"
    }
    ```

---

### 5.4 Administrative RBAC Endpoints (Requires `SUPER_ADMIN` Role)

These routes are protected by the dynamic RBAC middleware. The Gateway checks that the user's JWT has both the role `'SUPER_ADMIN'` and the specific permissions string before allowing access.

#### Endpoint 18: GET /api/v1/admin/users
*   **Behavior**: Lists all registered accounts in the system with full-name profiles, loyalty state, active lockout settings, and assigned roles. Fully paginated.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "Accounts retrieved successfully.",
      "data": [
        {
          "userId": "7b58c281-a5bf-4050-a922-a72a1cd40a92",
          "email": "john.doe@example.com",
          "isActive": true,
          "profile": {
            "fullName": "John Doe",
            "loyaltyTier": "SILVER"
          },
          "roles": ["CUSTOMER", "FLIGHT_ADMIN"]
        }
      ],
      "traceId": "tr-ad82x1ab-98ba"
    }
    ```
*   **Error Response — 403 Forbidden (Insufficient Privilege)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 403,
        "name": "FORBIDDEN",
        "message": "You do not have the required administrative permissions ('users:read') to access this resource."
      },
      "traceId": "tr-ad82x1ab-98ba"
    }
    ```

#### Endpoint 19: PUT /api/v1/admin/users/:userId/roles
*   **Request Body**:
    ```json
    { "roles": ["CUSTOMER", "FLIGHT_ADMIN"] }
    ```
*   **Behavior**: Updates the join table `UserRole` mappings. Revokes existing roles and binds the new ones, recalculating and updating the user's token permissions pool dynamically.
*   **Success Response — 200 OK**:
    ```json
    {
      "success": true,
      "message": "User authorization roles updated successfully.",
      "data": null,
      "traceId": "tr-adr82xa-81ba"
    }
    ```
*   **Error Response — 400 Bad Request (Invalid Roles Input)**:
    ```json
    {
      "success": false,
      "error": {
        "statusCode": 400,
        "name": "VALIDATION_ERROR",
        "message": "The role array provided contains invalid role names."
      },
      "traceId": "tr-adr82xa-81ba"
    }
    ```
*   **Error Response — 404 Not Found (User Not Found)**:
    ```json
    {
      "success": false,
      "error": {
        "code": "NOT_FOUND",
        "message": "The target user ID requested for role updates does not exist."
      },
      "traceId": "tr-adr82xa-81ba"
    }
    ```

---

### 5.5 Cluster Metadata & Observability

#### Endpoint 20: GET /.well-known/jwks.json
*   **Auth required**: No  
*   **Path note**: Public key directory.
*   **Success Response — 200 OK**:
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

#### Endpoint 21: GET /health
*   **Auth required**: No
*   **Success Response (Healthy) — 200 OK**:
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

---

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

// 6-digit numeric code validator helper
const otpCodeSchema = z
  .string()
  .length(6, 'Verification code must be exactly 6 digits')
  .regex(/^\d{6}$/, 'Verification code must contain only numbers');

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
  email: z.string().trim().toLowerCase().email(),
  code:  otpCodeSchema,
});

export const ResendVerificationSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
});

export const ForgotPasswordSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
});

export const ResetPasswordSchema = z.object({
  email:       z.string().trim().toLowerCase().email(),
  code:        otpCodeSchema,
  newPassword: passwordSchema,
});

export const ChangePasswordSchema = z.object({
  currentPassword: z.string().min(1, 'Current password is required'),
  newPassword:     passwordSchema,
});

export const UpdateProfileSchema = z.object({
  fullName: z.string().trim().min(2, 'Full name must be at least 2 characters').max(100),
});

// ─── MFA VALIDATION SCHEMAS ──────────────────────────────────────────────────
export const MfaVerifySchema = z.object({
  code: otpCodeSchema,
});

export const MfaLoginVerifySchema = z.object({
  mfaTicket: z.string().min(1, 'MFA Step-up ticket is required'),
  code:      otpCodeSchema,
});

// ─── ADMINISTRATIVE VALIDATION SCHEMAS ────────────────────────────────────────
export const AdminUpdateRolesSchema = z.object({
  roles: z.array(z.enum(['CUSTOMER', 'FLIGHT_ADMIN', 'SUPER_ADMIN'], {
    invalid_type_error: 'Roles must only contain CUSTOMER, FLIGHT_ADMIN, or SUPER_ADMIN'
  })).min(1, 'At least one role must be assigned'),
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
export type MfaVerifyInput            = z.infer<typeof MfaVerifySchema>;
export type MfaLoginVerifyInput       = z.infer<typeof MfaLoginVerifySchema>;
export type AdminUpdateRolesInput     = z.infer<typeof AdminUpdateRolesSchema>;
```

---

## 7. Layered Architecture & File Map

```
services/user-service/
│
├── prisma/
│   ├── schema.prisma              ← Decoupled IAM: User, UserProfile, Role, Permission, join tables, RefreshToken, OutboxEvent
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
import { PrismaClient } from '@prisma/client';
import bcrypt from 'bcrypt';

const prisma = new PrismaClient();

async function seed() {
  const rounds = parseInt(process.env.BCRYPT_ROUNDS ?? '12');

  console.log('Seeding security roles & permissions...');

  // 1. Create or Find roles
  const superAdminRole = await prisma.role.upsert({
    where: { name: 'SUPER_ADMIN' },
    update: {},
    create: { name: 'SUPER_ADMIN', description: 'System super administrator with full system controls' }
  });

  const flightAdminRole = await prisma.role.upsert({
    where: { name: 'FLIGHT_ADMIN' },
    update: {},
    create: { name: 'FLIGHT_ADMIN', description: 'Flight operation administrator' }
  });

  const customerRole = await prisma.role.upsert({
    where: { name: 'CUSTOMER' },
    update: {},
    create: { name: 'CUSTOMER', description: 'Standard flying passenger account' }
  });

  // 2. Seed default granular permissions
  const permissionsList = [
    { name: 'flights:read', desc: 'Allows viewing active flight schedules' },
    { name: 'flights:create', desc: 'Allows adding new flights to schedules' },
    { name: 'flights:delete', desc: 'Allows deleting/cancelling scheduled flights' },
    { name: 'bookings:read', desc: 'Allows reading flight passenger reservations' },
    { name: 'users:read', desc: 'Allows listing user registry for security reviews' },
    { name: 'users:ban', desc: 'Allows deactivating violating user accounts' }
  ];

  for (const perm of permissionsList) {
    await prisma.permission.upsert({
      where: { name: perm.name },
      update: {},
      create: { name: perm.name, description: perm.desc }
    });
  }

  // 3. Link permissions to roles
  const superAdminPerms = ['flights:read', 'flights:create', 'flights:delete', 'bookings:read', 'users:read', 'users:ban'];
  const flightAdminPerms = ['flights:read', 'flights:create', 'bookings:read'];
  const customerPerms = ['flights:read', 'bookings:read'];

  const bindPerms = async (roleName: string, permNames: string[]) => {
    const role = await prisma.role.findUnique({ where: { name: roleName } });
    if (!role) return;

    for (const name of permNames) {
      const perm = await prisma.permission.findUnique({ where: { name } });
      if (perm) {
        await prisma.rolePermission.upsert({
          where: { roleId_permissionId: { roleId: role.id, permissionId: perm.id } },
          update: {},
          create: { roleId: role.id, permissionId: perm.id }
        });
      }
    }
  };

  await bindPerms('SUPER_ADMIN', superAdminPerms);
  await bindPerms('FLIGHT_ADMIN', flightAdminPerms);
  await bindPerms('CUSTOMER', customerPerms);

  console.log('Seeding pre-verified administrator accounts...');

  const admins = [
    {
      name:  process.env.SUPER_ADMIN_NAME  ?? 'Super Admin',
      email: process.env.SUPER_ADMIN_EMAIL ?? 'superadmin@skyhub.com',
      pass:  process.env.SUPER_ADMIN_PASSWORD ?? 'SuperAdmin1!@#',
      roleId: superAdminRole.id,
    },
    {
      name:  process.env.FLIGHT_ADMIN_NAME  ?? 'Flight Admin',
      email: process.env.FLIGHT_ADMIN_EMAIL ?? 'flightadmin@skyhub.com',
      pass:  process.env.FLIGHT_ADMIN_PASSWORD ?? 'FlightAdmin1!@#',
      roleId: flightAdminRole.id,
    },
  ];

  for (const admin of admins) {
    const existing = await prisma.user.findUnique({ where: { email: admin.email } });
    if (existing) {
      console.log(`${admin.email} already exists — skipping seeding`);
      continue;
    }

    const passwordHash = await bcrypt.hash(admin.pass, rounds);
    
    // Create pre-verified user credentials, profile, and dynamic role link in a transaction
    await prisma.$transaction(async (tx) => {
      const user = await tx.user.create({
        data: {
          email: admin.email,
          passwordHash,
          isActive: true,
          emailVerified: true
        }
      });

      await tx.userProfile.create({
        data: {
          userId: user.id,
          fullName: admin.name,
          loyaltyTier: 'SILVER'
        }
      });

      await tx.userRole.create({
        data: {
          userId: user.id,
          roleId: admin.roleId
        }
      });
    });

    console.log(`Created administrator identity [${admin.email}]`);
  }

  await prisma.$disconnect();
  console.log('Seeding successfully completed!');
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
  async sendVerificationEmail(to: string, code: string): Promise<void> {
    await transporter.sendMail({
      from:    env.EMAIL_FROM,
      to,
      subject: 'Verify your SkyHub account',
      html:    `<p>Your SkyHub email verification code is: <strong>${code}</strong>. This code is valid for 10 minutes.</p>`,
    });
  },

  async sendPasswordResetEmail(to: string, code: string): Promise<void> {
    await transporter.sendMail({
      from:    env.EMAIL_FROM,
      to,
      subject: 'Reset your SkyHub password',
      html:    `<p>Your SkyHub password recovery code is: <strong>${code}</strong>. This code is valid for 10 minutes.</p>`,
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

> **`AppError` class & Global Handler** — Directly imported and re-exported from our compiled shared package `@skyhub/common-utils`.

**`src/middlewares/errorHandler.ts`:**
```typescript
export { globalErrorHandler } from '@skyhub/common-utils';
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
// Signing Access Tokens (attaching RBAC permissions dynamically)
async signAccessToken(userId, role, loyaltyTier, permissions): Promise<{ token: string, jti: string, expiresAt: Date }>
  → jose.SignJWT({ sub: userId, role, loyaltyTier, permissions, jti: generateJti() })
       .setProtectedHeader({ alg: 'RS256', kid: env.JWT_KEY_ID })
       .setExpirationTime('15m')
       .sign(privateKey)

// Creating a refresh token record (retains device metadata for revocation)
async createRefreshToken(userId, deviceInfo, ipAddress): Promise<string>
  → rawToken = generateRawToken(64)
  → tokenHash = hashToken(rawToken)
  → tokenRepository.create({ userId, tokenHash, deviceInfo, ipAddress, expiresAt: +7 days })
  → return rawToken  (only returned ONCE — caller sends to client)

// Rotating a refresh token (Token Rotation protection)
async rotateRefreshToken(oldTokenRaw, deviceInfo, ipAddress): Promise<{ newRaw, jti, accessToken, ... }>
  → hash = hashToken(oldTokenRaw)
  → existing = tokenRepository.findByHash(hash)
  → if not found → throw AppError('Session invalid or terminated', 401, 'INVALID_REFRESH_TOKEN')
  → if expired → throw AppError('Session expired', 401, 'INVALID_REFRESH_TOKEN')
  → tokenRepository.deleteById(existing.id)
  → return createRefreshToken + signAccessToken for existing.userId
```

**`src/services/auth.service.ts`** — core logic:
```typescript
async register(name, email, password): ...
  → check duplicate email
  → hash password using bcrypt (rounds=12)
  → generate cryptographically secure 6-digit OTP: crypto.randomInt(100000, 999999).toString()
  → SHA-256 hash the OTP code and set expiry to 10 minutes
  → DB transaction: create user credentials + user profile (Silver tier) + outbox event (USER_REGISTERED)
  → send verification email with raw 6-digit OTP
  → return user profile metadata

async login(email, password, deviceInfo, ip): ...
  → find user by email (timing-safe Bcrypt compare against dummy hash for non-existent users)
  → check is_active, email_verified, locked_until (block if locked)
  → bcrypt.compare(password, user.passwordHash)
  → handle wrong password (increment counter, lock for 30 minutes on 5 consecutive failures)
  → check if user has mfa_enabled = true
       - If MFA enabled: generate temporary short-lived mfaTicket and return mfaRequired = true
       - If MFA disabled: create and return final access token (with permissions scopes) + refresh token
  → update last_login_at
  → write USER_LOGGED_IN event to outbox

async verifyEmail(email, code): ...
  → look up user by email, verify code SHA-256 hash matches and is not expired
  → update emailVerified = true in credentials table

async forgotPassword(email): ...
  → generate 6-digit password recovery OTP, SHA-256 hash it, set 10-minute expiry
  → email raw OTP code to user (always return success to prevent account enumeration)

async resetPassword(email, code, newPassword): ...
  → verify SHA-256 hash of recovery code matches email, verify newPassword complies with Zod rules
  → update password hash in credentials database, clear reset tokens

async changePassword(userId, currentPassword, newPassword): ...
  → verify currentPassword match, verify newPassword is not identical, update password hash

async logout(userId, jti, rawRefreshToken): ...
  → delete specific refresh token hash, blacklist JWT jti in Redis (TTL = remaining exp)

async logoutAll(userId, jti): ...
  → delete all refresh tokens in DB for user, blacklist current jti in Redis
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

// ─── 1. STANDARD PUBLIC ROUTING ──────────────────────────────────────────────
router.post('/register',            validate(schemas.RegisterSchema),           authController.register);
router.post('/verify-email',        validate(schemas.VerifyEmailSchema),        authController.verifyEmail);
router.post('/resend-verification', validate(schemas.ResendVerificationSchema), authController.resendVerification);
router.post('/login',               validate(schemas.LoginSchema),              authController.login);
router.post('/refresh',             validate(schemas.RefreshTokenSchema),       authController.refresh);
router.post('/forgot-password',     validate(schemas.ForgotPasswordSchema),     authController.forgotPassword);
router.post('/reset-password',      validate(schemas.ResetPasswordSchema),      authController.resetPassword);

// ─── 2. STANDARD PROTECTED ROUTING (Identity Vault) ─────────────────────────
router.post('/logout',              requireAuth, authController.logout);
router.post('/logout-all',          requireAuth, authController.logoutAll);
router.get('/me',                   requireAuth, authController.getProfile);
router.put('/me',                   requireAuth, validate(schemas.UpdateProfileSchema), authController.updateProfile);
router.post('/change-password',     requireAuth, validate(schemas.ChangePasswordSchema), authController.changePassword);

// ─── 3. DEVICE-AWARE ACTIVE SESSION MANAGEMENT (Security Center) ────────────
router.get('/sessions',             requireAuth, authController.listSessions);
router.delete('/sessions/:sessionId',requireAuth, authController.revokeSession);

// ─── 4. TIME-BASED MULTI-FACTOR AUTHENTICATION (TOTP MFA) ───────────────────
router.post('/mfa/enable',          requireAuth, authController.enableMfa);
router.post('/mfa/verify',          requireAuth, validate(schemas.MfaVerifySchema), authController.verifyMfa);
router.post('/mfa/login-verify',    validate(schemas.MfaLoginVerifySchema), authController.loginVerifyMfa);

// ─── 5. ADMINISTRATIVE CONTROL ROUTING (Dynamic RBAC Vault) ─────────────────
router.get('/admin/users',          requireAuth, authController.adminListUsers);
router.put('/admin/users/:userId/roles', requireAuth, validate(schemas.AdminUpdateRolesSchema), authController.adminUpdateUserRoles);

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
    expect(res.body.success).toBe(false);
    expect(res.body.error.statusCode).toBe(409);
    expect(res.body.error.name).toBe('CONFLICT');
  });

  it('returns 400 for weak password', async () => {
    const res = await request(app).post('/api/v1/auth/register').send({
      name: 'Test', email: 'weak@example.com', password: 'weak',
    });
    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error.statusCode).toBe(400);
    expect(res.body.error.name).toBe('VALIDATION_ERROR');
  });
});
```

**Validation checklist before marking a step complete:**
- `POST /register` → 201 Created, profiles populated inside `user_profiles`, email pre-deactivated.
- `POST /register` with same email → 409 Conflict with `{ statusCode: 409, name: 'CONFLICT' }` payload.
- `POST /login` before email verification → 401 Unauthorized with `{ statusCode: 401, name: 'EMAIL_NOT_VERIFIED' }`.
- `POST /verify-email` with valid 6-digit OTP code → 200 OK.
- `POST /login` after verification (MFA inactive) → 200 OK with rotated RS256 access and refresh tokens.
- `POST /login` (MFA active) → 200 OK with `{ mfaRequired: true, mfaTicket: "..." }` redirect.
- `GET /me` with Bearer token → 200 OK with decoupled profile details (`fullName`, `loyaltyTier`, `bookingCount`).
- `POST /refresh` → 200 OK with rotated refresh tokens (Token Rotation security verified).
- `POST /logout` → 200 OK, JTI token blacklisted in Redis cluster.
- `GET /sessions` & `DELETE /sessions/:sessionId` → 200 OK, active session listed and terminated.
- 5× wrong password → account locked for 30 minutes → 423 ACCOUNT_LOCKED with remaining numeric seconds inside body details.
- `POST /forgot-password` → 200 OK, emails secure 6-digit password reset OTP (decoy returned on non-existent accounts).
- `POST /reset-password` with valid OTP code → 200 OK, updates hash in credential vault.

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
  it('throws 409 AppError when email already registered', async () => {
    userRepository.findByEmail.mockResolvedValue({ id: '123' });
    await expect(authService.register({ name: 'A', email: 'exists@test.com', password: 'X' }))
      .rejects.toMatchObject({ statusCode: 409, name: 'CONFLICT' });
  });
});
```

### Integration Tests (Full HTTP — real test DB)

Use a separate `skyhub_user_test_db` database. Reset tables before each test suite:
```typescript
beforeEach(async () => {
  await prisma.rolePermission.deleteMany();
  await prisma.userRole.deleteMany();
  await prisma.permission.deleteMany();
  await prisma.role.deleteMany();
  await prisma.refreshToken.deleteMany();
  await prisma.userProfile.deleteMany();
  await prisma.outboxEvent.deleteMany();
  await prisma.user.deleteMany();
});
```

### What to Test

| Area | Test Cases |
|---|---|
| **Registration** | Valid parameters, duplicate email conflicts, weak password validation. |
| **Email Verification** | Valid 6-digit OTP code, expired code, invalid format (non-numeric / length). |
| **Login Verification** | Correct credentials, wrong password lock counters, unverified accounts, locked accounts, step-up MFA login ticket redirection. |
| **Account Lockout** | 5 failed attempts locks account for 30 minutes, locked account queries return dynamic `secondsRemaining`. |
| **Active Session Management** | Retrieve list of active sessions, revoking custom `sessionId` terminates refresh, other devices remain unrevoked. |
| **TOTP Authenticator MFA** | Setup key and QR code generation, verifying code enables MFA, logging in with MFA active forces step-up authentication. |
| **Token Refresh** | Token rotation successfully rotates credentials, expired refresh tokens blocked, replay attack detection on rotated tokens. |
| **Logout Flows** | Single session logout blacklists JTI in Redis, global logout kills all sessions in database. |
| **Password Recovery** | Forgot password triggers 6-digit OTP recovery mail, reset password updates hash using OTP code, new password same as current password rule validation. |
| **Profile Metadata** | Reading `/me` fetches credentials-free public profiles, PUT updates `fullName` correctly, unauthenticated requests blocked. |
| **Loyalty upgrades** | Upgrades SILVER $\rightarrow$ GOLD at 5 bookings, and GOLD $\rightarrow$ PLATINUM at 15 bookings via Kafka listeners, writing outbox upgrade events. |

---

## Quick Reference: Build Checklist

```
Step 1  ✓  Package.json, tsconfig.json, Prisma schema mapped, .env created
Step 2  ✓  PostgreSQL migrate dev run, Dynamic RBAC, User & Profile tables seeded
Step 3  ✓  Singletons (DB, Redis, Kafka, logger, keys), middlewares, compiled common-utils errors & envelopes
Step 4  ✓  User, Token, Outbox repositories implemented and tested
Step 5  ✓  AuthService, TokenService, LoyaltyService with dynamic JWT scopes and 6-digit OTPs
Step 6  ✓  Controllers, Routes (all 21 endpoints mapped), app.ts, server.ts boots, /health ok
Step 7  ✓  OutboxWorker running, Kafka event listener processing upgrades
Step 8  ✓  JWKS endpoint returning public key in JWK format
Step 9  ✓  Integration tests pass for all 12 validation checklist items
```

Once all 9 steps pass their validation, Phase 1 (User Service) is complete and production-ready. Proceed to Phase 2: Flight Service.

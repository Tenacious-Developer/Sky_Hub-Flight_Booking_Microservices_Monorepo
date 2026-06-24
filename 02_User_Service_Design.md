# SkyHub — User Service: Complete Production-Grade Build Guide

## Table of Contents

1. [Bounded Context & Responsibility](#1-bounded-context--responsibility)
2. [Database Design & Prisma Schema](#2-database-design--prisma-schema)
3. [Security & RBAC Architecture](#3-security--rbac-architecture)
4. [Complete REST API Specification](#4-complete-rest-api-specification)
5. [Zod Validation Schemas](#5-zod-validation-schemas)
6. [Kafka Event Publishing (Outbox Pattern)](#6-kafka-event-publishing-outbox-pattern)
7. [Layered Architecture & File Map](#7-layered-architecture--file-map)
8. [npm Dependencies](#8-npm-dependencies)
9. [Environment Variables](#9-environment-variables)
10. [Step-by-Step Build Plan](#10-step-by-step-build-plan)
11. [Testing Strategy](#11-testing-strategy)

---

## 1. Bounded Context & Responsibility

### 1.0 What This Service Is — IAM in Plain Terms

This service is an **IAM (Identity & Access Management)** system. Strip away the jargon and it exists to answer four questions about every request that touches SkyHub:

| Question | The term | Plain meaning |
| :--- | :--- | :--- |
| **Who are you?** | **Identity** | A real person exists and we hold a record of them. |
| **Can you prove it?** | **Authentication (authN)** | You presented something only you should have — password, OTP, authenticator code. |
| **What are you allowed to do?** | **Authorization (authZ)** | A `CUSTOMER` can book a flight; only a `FLIGHT_ADMIN` can cancel one. |
| **What did you do?** | **Accountability / Audit** | An immutable record of sensitive actions, for security and compliance. |

The User Service is the **authoritative identity provider** for the entire SkyHub cluster — the single source of truth for identity. Every other service (Flight, Booking, Payment) is a *consumer* of identity, never a producer. The Flight Service never checks a password; it receives "this request is from customer #123, role `CUSTOMER`" and trusts it, because the User Service already vouched for it.

**Two distinctions that unlock the whole design:**

1. **Authentication vs Authorization.** AuthN proves *who you are* and happens **once** at login. AuthZ checks *what you may do* and happens on **every** request afterward. You authenticate once, then carry proof (a token) that gets you authorized many times.
2. **Credential vs Token.** A **credential** (your password) is the long-lived secret that proves identity from scratch — high value, used rarely (only at login), guarded with bcrypt and never stored readably. A **token** is the short-lived, disposable proof issued *after* you use the credential — used constantly, on every request, and built to be cheap to verify and fast to expire. **The entire service is a negotiation around one trade: use the precious credential rarely, use cheap disposable tokens constantly.**


### 1.5 The Feature Catalog — In Depth

Every feature is grouped by the IAM question it answers, with the *problem it solves*. Scope is marked **[v1]** (build now) vs **[v2/v3]** (defer to Phase 7) — honor that line; building the v3 features now will drown the v1 work.

#### A. Identity lifecycle — "creating and managing who you are"

- **Registration + Email Verification [v1]** — *Problem:* anyone can type any email; how do we know you own it? *Solution:* create the account in a not-yet-verified state, email a 6-digit OTP, and block login until you prove you can read that inbox. First line against bot/spam accounts. Creates a `User` + `UserProfile` in one transaction; emits `USER_REGISTERED`.
- **Profile Management [v1]** — *Problem:* credentials are sensitive, display info (name, tier) is not. *Solution:* a separate `user_profiles` row read/written via `GET/PUT /me` *without ever touching the credential vault* — fetching your name can never leak your password hash.
- **Email Verification & Resend [v1]** — the OTP issue/check/rate-limit machinery behind registration: SHA-256-hashed OTP, 10-min expiry, 5-attempt cap, resend limited to 1/2 min.

#### B. Authentication — "proving it's you, safely"

- **Login + Account Lockout [v1]** — *Problem:* attackers brute-force passwords by the million. *Solution:* a deliberately slow bcrypt compare, 5-attempt lockout (30 min), and responses crafted so an attacker learns nothing about whether an email exists (post-compare state checks, anti-enumeration). The most security-dense endpoint in the system.
- **Token Issuance (access + refresh) [v1]** — *Problem:* you can't send your password on every request. *Solution:* on login, mint a short-lived RS256 access token (15 min, the boarding pass) and a long-lived refresh token (7 days, the visa).
- **Token Refresh + Rotation [v1]** — *Problem:* access tokens expire every 15 min — users shouldn't re-login constantly. *Solution:* trade the refresh token for a fresh access token, *rotating* (replacing) the refresh token each use; the atomic delete is the concurrency guard, so a replayed/stolen token self-destructs → 401. *(v2: reuse detection revokes all.)*
- **Logout (single & all sessions) [v1]** — *Problem:* "log me out" must kill a stateless access token that's still technically valid. *Solution:* delete the refresh token(s) and blacklist the access-token `jti` in Redis until its natural expiry.

#### C. Credential recovery & hygiene — "what happens when things go wrong"

- **Forgot / Reset Password [v1]** — *Problem:* people forget passwords; attackers exploit reset flows for account takeover. *Solution:* a 6-digit recovery OTP, an anti-enumeration decoy `200` whether or not the email exists, and a reset that kills **all** sessions (a hijacker gets kicked out).
- **Change Password (authenticated) [v1]** — *Problem:* a logged-in user rotating their password, possibly suspecting compromise. *Solution:* verify the current password, forbid reuse, then sign out all *other* devices while keeping the current one.

#### D. Authorization — "what you're allowed to do"

- **RBAC (Role-Based Access Control) [v1 static / v3 dynamic]** — *Problem:* a customer must not cancel flights or ban users. *Solution (v1):* a static `role` column (`CUSTOMER` / `FLIGHT_ADMIN` / `SUPER_ADMIN`) drives the JWT `role` claim; services check it. *(v3 upgrades to fine-grained `permissions[]` from dedicated `roles`/`permissions` tables — see §3.6.)*
- **JWKS Endpoint [v1]** — *Problem:* every service must *verify* tokens, but only this service can *sign* them. *Solution:* `GET /.well-known/jwks.json` publishes the RS256 *public* key so any service verifies tokens independently — the trust anchor for the whole cluster.

#### E. Domain integration — "User Service inside the bigger system"

- **Loyalty Tier System [v1 consumer]** — *Problem:* completed bookings should promote SILVER→GOLD→PLATINUM, but this service doesn't own bookings. *Solution:* an idempotent `BOOKING_COMPLETED` consumer increments `booking_count`, recalculates tier, emits `USER_LOYALTY_UPDATED` (see §6). First taste of event-driven, cross-service communication.
- **Kafka Event Publishing (Outbox) [v1]** — *Problem:* identity events must reliably reach other services even if Kafka is briefly down. *Solution:* write the event into the DB in the same transaction as the state change; a background worker ships it to `user-identity-events` later — at-least-once reliability through your own database (see §6).

#### F. Advanced security — **deferred to v3**

- **Device-Aware Session Management [v3]** — list/revoke active sessions parsed from `User-Agent`; `isCurrent` via `last_jti`.
- **TOTP MFA [v3]** — optional authenticator-app step-up; `MFA_REQUIRED` ticket flow at login.
- **Dynamic RBAC (permissions tables) [v3]** — fine-grained scopes (`flights:create`) instead of coarse roles (§3.6).
- **Security Audit Logging [v3]** — immutable structured audit trail for sensitive operations (§3.7).

#### G. Operational

- **Health Check [v1]** — `GET /api/v1/health` — DB + Redis + Kafka liveness for readiness probes.

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

### 1.6 End-to-End Request Flows (Client → Gateway → Service → Return)

These are the canonical step-by-step flows for the two foundational endpoints, mirrored from `01_Architecture.md` §4.1. They show exactly where each responsibility lives: rate-limiting and correlation IDs at the **Gateway**, all identity/security logic in the **User Service**, and event publication via the **background Outbox Worker**.

#### Registration Flow

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
```

#### Login Flow

```text
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

> **Note on the login step order:** the simplified flow above (from the architecture overview) lists the `is_active`/`email_verified` check at step 3 for readability. The **authoritative, anti-enumeration-correct order is in §4 Feature 2**: lockout check → `bcrypt.compare` (with a dummy hash for non-existent users) → **then** the account-state checks *post-compare*. Build to §4 Feature 2, not to the simplified diagram.

---

## 2. Database Design & Prisma Schema

To align with modern industry-standard designs for **Identity & Access Management (IAM)** and to ensure strict security, this database architecture separates **Core Authentication Credentials**, **Public Profile metadata**, **Active Sessions**, **Security Audit Logs**, and **Granular Authorization (Dynamic RBAC)** into distinct decoupled tables.

### 2.0 How to Read This Schema (Design Philosophy — read this first)

#### The one principle that explains every table

> **Separate data by its *sensitivity*, its *access pattern*, and its *blast radius* — not just by "it's all about a user."**

A beginner builds one giant `users` table (name, password, role, last_login, …). Every serious IAM system (Auth0, Okta, AWS Cognito, Keycloak) instead splits identity into decoupled tables, because each table isolates a specific risk or query pattern. This schema has **five conceptual zones**:

```
   THE VAULT          THE STOREFRONT      THE KEYRING        THE RULEBOOK         THE LEDGER
   ┌─────────┐        ┌──────────────┐    ┌──────────────┐   ┌──────────────┐    ┌────────────┐
   │  users  │  1:1   │ user_profiles│    │refresh_tokens│   │ roles/perms  │    │ audit_logs │
   │(secrets)│────────│  (public)    │    │  (sessions)  │   │   (authz)    │    │(accountab.)│
   └─────────┘        └──────────────┘    └──────────────┘   └──────────────┘    └────────────┘
   highly sensitive   safe to read freely  medium-sensitive   slow-changing       append-only
   read rarely        read constantly      read per-refresh   read per-login      write-once
```

| Zone | Tables | Why it is its own zone |
| :--- | :--- | :--- |
| **The Vault** | `users` | Credentials + security state only (`password_hash`, lockout, MFA, OTP tokens). Touched only at login/register/password-change so routine reads never go near secrets. |
| **The Storefront** | `user_profiles` | Public, safe-to-read data (`full_name`, `loyalty_tier`). Read on every profile view — kept out of the Vault so a name fetch can never leak a password hash. |
| **The Keyring** | `refresh_tokens` | One row = one logged-in device/session. Layout *is* the feature: logout-all = `DELETE WHERE user_id`, list-sessions = `SELECT WHERE user_id`. |
| **The Rulebook** | `roles`, `permissions`, `user_roles`, `role_permissions` | Authorization (RBAC). Slow-changing reference data + the M:N wiring between them. |
| **The Ledger** | `audit_logs` | Accountability. Append-only, write-once — an audit trail you can edit is worthless. |
| *(infra)* | `outbox_events`, `processed_events` | Not identity data — reliable event publishing + consumer idempotency (see §6). |

#### All 10 tables are PHYSICAL tables in PostgreSQL

There is no "logical-only" or "code-only" table here. Every model in §2.3 becomes a real table on disk when you migrate — **including the two join tables.** A *join table* (junction/bridge table) is physical storage; a SQL `JOIN` is the runtime query operation that reads across it. Different things:

| Term | What it is | Where it lives |
| :--- | :--- | :--- |
| **Join table** (`user_roles`, `role_permissions`) | A physical table storing pairs of IDs | In the DB, on disk — you can see/edit its rows in `db:studio` |
| **A `JOIN`** (SQL keyword) | A query operation combining rows at read time | In your code / query |

| # | Table | Zone | Physical? |
| :-- | :--- | :--- | :--- |
| 1 | `users` | Vault | ✅ |
| 2 | `user_profiles` | Storefront | ✅ |
| 3 | `refresh_tokens` | Keyring | ✅ |
| 4 | `roles` | Rulebook | ✅ |
| 5 | `permissions` | Rulebook | ✅ |
| 6 | `user_roles` *(join)* | Rulebook | ✅ |
| 7 | `role_permissions` *(join)* | Rulebook | ✅ |
| 8 | `audit_logs` | Ledger | ✅ |
| 9 | `outbox_events` | infra | ✅ |
| 10 | `processed_events` | infra | ✅ |

#### Relationships & cardinality (how the tables connect)

| Relationship | Cardinality | How it's modeled |
| :--- | :--- | :--- |
| `users` → `user_profiles` | **1:1** | Child's FK *is* its PK (`userId @id`) — one column, and structurally forbids two profiles per user. |
| `users` → `refresh_tokens` | **1:N** | FK `user_id` on the many-side; one user, many device sessions. |
| `users` ↔ `roles` | **M:N** | Join table `user_roles` `(user_id, role_id)`. |
| `roles` ↔ `permissions` | **M:N** | Join table `role_permissions` `(role_id, permission_id)`. |
| `users` → `audit_logs` | **1:N** | FK `user_id`, but `onDelete: SetNull` (history outlives the user). |

#### Why many-to-many always needs THREE tables (the RBAC wiring)

A relational row can't cleanly hold "a list of roles." So an M:N relationship is **always** the two endpoint tables **plus a join table** of `(id, id)` pairs. A join table cannot exist alone — each of its columns is a foreign key, so both parent tables must exist first (this also fixes migration order: `roles` + `permissions` are created *before* the join tables that reference them). Concrete rows:

```
users                roles                 user_roles  (the join table — real rows on disk)
┌────┬───────────┐   ┌────┬──────────────┐ ┌─────────┬─────────┐
│ u1 │ vivek     │   │ r1 │ CUSTOMER     │ │ user_id │ role_id │
│ u2 │ admin     │   │ r2 │ FLIGHT_ADMIN │ │   u1    │   r1    │  ← Vivek is CUSTOMER
└────┴───────────┘   └────┴──────────────┘ │   u2    │   r1    │  ← admin is CUSTOMER
                                           │   u2    │   r2    │  ← admin is ALSO FLIGHT_ADMIN
                                           └─────────┴─────────┘
```

Two design details on every join table:
1. **Composite primary key** (`@@id([userId, roleId])`) — the *pair* is the identity and is unique by definition, so a user can't be assigned the same role twice.
2. **An index on the *second* column** (`@@index([roleId])`) — the PK index leads with `userId`, so it answers "what roles does this user have?" fast, but **not** the inverse "which users have this role?". A composite index only serves queries filtering on its *leading* column(s); the reverse direction needs its own index.

#### "Exists" vs "is used" — the v1/v3 line

The v1/v3 split is about **runtime behavior, not table existence.** All 10 tables (and the seed data) ship from day one — the `seed.ts` in §10 even populates `roles`/`permissions`/`role_permissions`/`user_roles`. What's deferred to v3 is only the *login code that reads these tables to stamp a `permissions[]` claim into the JWT.* In v1, login signs the `role` claim from the **static `users.role` enum column** and never touches the RBAC tables — they sit present-and-seeded but dormant.

```
   v1 (now):  login reads  users.role  ──────────────► JWT { role }
   v3 (later): login JOINs user_roles → role_permissions ──► JWT { role, permissions[] }
              (same tables, now read at runtime)
```

#### The 8 design instincts to carry everywhere

1. **Split tables by sensitivity + access pattern + blast radius**, not by "it's about a user."
2. **UUID PKs** for anything externally referenced — anti-enumeration (`/users/1,2,3` walks your whole base) and distributed-friendly.
3. **Store hashes, and match the hash to the threat** — bcrypt for low-entropy passwords (slow = brute-force-resistant); SHA-256 for high-entropy tokens (already unguessable, no slow hash needed).
4. **`UNIQUE` is a business assertion ("this can never repeat")** — refresh-token hash is UNIQUE (huge entropy); OTP token hash is deliberately NOT (6-digit codes collide; a UNIQUE constraint would break the second user).
5. **Make illegal states unrepresentable** — FK-as-PK for 1:1; composite PK on join tables.
6. **`onDelete` encodes policy** — `Cascade` = "meaningless without the parent" (profile, tokens); `SetNull` = "outlives the parent" (audit logs).
7. **Composite indexes serve leading-column queries only** — add an inverse index for the reverse direction.
8. **Soft delete, never hard delete identity** — `is_active` (ban) + `deleted_at` (GDPR) preserve referential integrity and history.

#### Real-world parallel (this is the canonical IAM model, not a SkyHub invention)

- **Keycloak:** `user_entity`, `credential`, `user_session`, `keycloak_role` + `user_role_mapping` (this exact join-table pattern), `event_entity` (audit) — this schema is essentially a clean, learnable Keycloak.
- **Auth0 / Okta:** identity vs metadata vs sessions/grants vs roles & permissions vs immutable logs — the same five zones.
- **AWS Cognito:** User Pool (identity + attributes), token/session handling, groups (≈ roles).

### 2.1 Entity-Relationship Diagram

```
                              ┌────────────────────────┐
                              │         USERS          │
                              │ (Core Auth Credentials)│
                              ├────────────────────────┤
                              │ id (UUID) [PK]         │
                              │ email (VARCHAR) [UQ]   │
                              │ password_hash (VARCHAR)│
                              │ role (ENUM) ← v1 static│
                              │ is_active (BOOLEAN)    │
                              │ failed_attempts (INT)  │
                              │ locked_until (TIMESTAMPTZ)
                              │ mfa_enabled (BOOLEAN)  │
                              │ mfa_secret (VARCHAR)   │
                              └───────────┬────────────┘
                                          │ 1
                                          │
                  ┌───────────────────────┼───────────────────────┐
                  │ 1                     │ N                     │ N
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

**Note on `created_by` / audit trust:** like the Flight Service, this service trusts the `X-User-Id` / `X-User-Role` headers injected by the Gateway (which has already verified the JWT). It does NOT re-verify the JWT on each request.

### 2.2 Column-by-Column Justification

#### `users` (Core Identity & Security Credentials)
This table acts as the vault. It only handles identity verification, multi-factor settings, security lockout metrics, and account status/lifecycles.

| Column | Type | Why This Design Choice |
| :--- | :--- | :--- |
| `id` | UUID | Globally unique, safe from business intelligence leaks (auto-increment integers leak scale). |
| `email` | VARCHAR(255) | B-Tree indexed and unique. Used as the unique login handle. |
| `password_hash` | VARCHAR(255) | Holds the highly secure Bcrypt hash (rounds=12). Never loaded during profile requests. |
| `is_active` | BOOLEAN | Allows administrative soft deactivation (e.g. banning) without wiping audit histories. |
| `role` | ENUM (`AccountRole`) | **v1 static role** (`CUSTOMER \| FLIGHT_ADMIN \| SUPER_ADMIN`, default CUSTOMER) — the JWT `role` claim is signed from this column. v3's dynamic RBAC tables add the `permissions` claim later; this stays as the coarse role. |
| `email_verify_attempts` / `reset_attempts` | INT | Enforce the 5-wrong-codes cap — a 6-digit OTP space (900k) is online-brute-forceable without it. Reset to 0 when a new code is issued. |
| `failed_login_attempts` | INT | Lockout tracker. Incremented on wrong passwords, reset on success. |
| `locked_until` | TIMESTAMPTZ | Absolute lock expiration time. The auth pipeline verifies `locked_until > NOW()`. |
| `mfa_enabled` | BOOLEAN | Indicates if the user has completed authenticator TOTP setup. |
| `mfa_secret` | VARCHAR(255) | Stores the **encrypted-at-rest** Base32 TOTP secret (v3). Encrypt with **AES-256-GCM** using a dedicated key from `MFA_ENCRYPTION_KEY` env var (separate from the JWT keys); store `iv:authTag:ciphertext`. Unlike passwords/OTPs, the TOTP secret must be **reversible** (the server re-derives the current code to compare), so it is *encrypted*, never hashed — and the encryption key lives outside the DB so a DB leak alone can't recover secrets. The VARCHAR(255) sizes for the encoded ciphertext bundle. |
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

### 2.3 Complete Production-Grade Prisma Schema

**File: `services/user-service/src/db/schema.prisma`**

> **Prisma 7 layout (follow the flight-service convention):** the schema lives at `src/db/schema.prisma`, migrations at `src/db/migrations/`, and the generated client at `src/db/generated/prisma/` (gitignored). The datasource `url` is **not** in the schema — it comes from `src/config/prisma.config.ts` (Prisma 7 `defineConfig`), and every Prisma CLI command runs with `--config src/config/prisma.config.ts` (wrapped by the `db:*` npm scripts). At runtime the client is constructed with the `pg` driver adapter: `new PrismaClient({ adapter: new PrismaPg(pool) })`.
>
> **Generator must be `prisma-client` (modern ESM), NOT `prisma-client-js` (legacy).** The runtime imports `PrismaClient`/`Prisma` from the generated `client` entrypoint (`src/db/generated/prisma/client`), which only the modern `prisma-client` generator produces. Using `prisma-client-js` writes a different layout that the `/client` import never picks up, so newly-added models silently fail to appear.
>
> **Migrations:** `npm run db:migrate` (= `prisma migrate dev`) is interactive — run it in a real terminal. In non-interactive contexts apply migrations with `prisma migrate deploy` instead.

```prisma
generator client {
  provider = "prisma-client"
  output   = "./generated/prisma"
}

datasource db {
  provider = "postgresql"
}

// ─── 1. CORE AUTHENTICATION (The Credential Vault) ───────────────────────────
model User {
  id                   String        @id @default(uuid())
  email                String        @unique
  passwordHash         String        @map("password_hash")
  isActive             Boolean       @default(true) @map("is_active")

  // v1 static role — the JWT `role` claim is signed from THIS column (see Build Scope).
  // v3's dynamic RBAC tables below add the `permissions` claim later; this column
  // remains the coarse role even after v3.
  role                 AccountRole   @default(CUSTOMER)

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
  emailVerifyAttempts  Int           @default(0) @map("email_verify_attempts") // 5 wrong codes → invalidate code (§4)
  resetToken           String?       @map("reset_token") // SHA-256 hash of OTP
  resetExpiresAt       DateTime?     @map("reset_expires_at")
  resetAttempts        Int           @default(0) @map("reset_attempts")        // same 5-attempt cap (§4)

  // Audit and lifecycle
  createdAt            DateTime      @default(now()) @map("created_at")
  updatedAt            DateTime      @updatedAt @map("updated_at")
  deletedAt            DateTime?     @map("deleted_at")

  // Relations
  profile              UserProfile?
  refreshTokens        RefreshToken[]
  userRoles            UserRole[]
  auditLogs            AuditLog[]

  // NOTE: no @@index([email]) — @unique above already creates that index
  @@index([emailVerifyToken])
  @@index([resetToken])
  @@map("users")
}

enum AccountRole {
  CUSTOMER
  FLIGHT_ADMIN
  SUPER_ADMIN
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
  lastJti    String?  @map("last_jti") // jti of the latest access token issued via this session — powers `isCurrent` in GET /sessions
  deviceInfo String?  @map("device_info")
  ipAddress  String?  @map("ip_address")
  expiresAt  DateTime @map("expires_at")
  createdAt  DateTime @default(now()) @map("created_at")

  user       User     @relation(fields: [userId], references: [id], onDelete: Cascade)

  @@index([userId])
  @@map("refresh_tokens")
}

enum OutboxStatus {
  PENDING
  PROCESSING
  PUBLISHED
  FAILED
}

// Same shape + worker behaviour as Flight Service (doc 04 §6.4):
// PROCESSING + updated_at power the stale-PROCESSING reclaim, retry_count caps retries.
model OutboxEvent {
  id          String       @id @default(uuid())
  eventType   String       @map("event_type")
  payload     Json
  status      OutboxStatus @default(PENDING)
  retryCount  Int          @default(0) @map("retry_count")
  createdAt   DateTime     @default(now()) @map("created_at")
  updatedAt   DateTime     @updatedAt @map("updated_at")    // powers the stale-PROCESSING reclaim
  publishedAt DateTime?    @map("published_at")

  @@index([status, createdAt])
  @@map("outbox_events")
}

// Consumer-side dedupe: Kafka delivery is at-least-once — every consumed event's ID
// is recorded in the SAME transaction as its side effect, so replays are no-ops.
// (Used by the BOOKING_COMPLETED consumer — see §6.5.)
model ProcessedEvent {
  eventId     String   @id @map("event_id")
  processedAt DateTime @default(now()) @map("processed_at")

  @@map("processed_events")
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

### 2.4 Database Indexes Summary

| Table | Index | Type | Purpose |
| :--- | :--- | :--- | :--- |
| `users` | `email` | B-Tree UNIQUE | Exact match credential lookups during authentication. |
| `users` | `email_verify_token` | B-Tree (non-unique) | One-way hash verification lookup. Deliberately NOT unique — 6-digit OTPs are low-entropy, two users can hold the same code (see §2.2). |
| `users` | `reset_token` | B-Tree (non-unique) | Password recovery code validation. Same non-unique rationale. |
| `user_profiles` | `user_id` | B-Tree UNIQUE | Dynamic 1-to-1 fetching for metadata. |
| `roles` | `name` | B-Tree UNIQUE | Role checking constraint. |
| `permissions` | `name` | B-Tree UNIQUE | Permission checking constraint. |
| `refresh_tokens` | `token_hash` | B-Tree UNIQUE | O(1) matching on `/refresh` session validations. |
| `refresh_tokens` | `user_id` | B-Tree | Locates all sessions for single/global logout. |
| `outbox_events` | `(status, created_at)` | Composite B-Tree | High-speed polling query indexing. |

*Note on Outbox Worker Polling Optimization*: Although the Prisma schema defines a compound index `(status, createdAt)` for database-engine compatibility, in a PostgreSQL production environment it is highly recommended to replace it with a **partial index** in the SQL migration file:
```sql
CREATE INDEX idx_pending_outbox ON outbox_events (created_at) WHERE status = 'PENDING';
```
This keeps the index size minimal by only indexing active, unprocessed outbox tasks.

### 2.5 Schema Evolution & Migration Strategy

The full schema above is the *finished* v3 shape; v1 ships a subset (see Build Scope). Stage the growth with zero-downtime patterns:

- **Expand-Contract for additive columns (v2 lockout/OTP fields, v3 MFA columns):**
  1. Apply the migration to add the column (nullable or with a safe default).
  2. Deploy the updated code that reads/writes it.
- **Dropping/renaming columns:** deploy code that stops using the old column first, then drop it in a later migration.
- **v3 RBAC tables (`roles`/`permissions`/join tables):** either create them empty in the v1 migration (cheap, dormant) or add them later as a genuine additive migration — both are valid; **never** build the v3 endpoints during v1.

---

## 3. Security & RBAC Architecture

### 3.1 Password Hashing — bcrypt

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

### 3.2 RS256 JWT — Asymmetric Signing

**Key pair generation (run once, store in environment):**
```bash
# genpkey outputs PKCS#8 ("-----BEGIN PRIVATE KEY-----") — REQUIRED by jose's importPKCS8().
# Do NOT use `openssl genrsa` — it outputs PKCS#1 ("BEGIN RSA PRIVATE KEY"), which importPKCS8() rejects at startup.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out private.pem
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

`sub` = userId. `jti` = unique token ID (used for blacklisting). No email, no name — minimise PII in tokens. The `permissions` array is a **v3** addition — until dynamic RBAC exists, the token carries `role` only (see Build Scope).

**Why `jose` library instead of `jsonwebtoken`?**
- `jsonwebtoken` has no RS256 JWKS support built in
- `jose` is the modern IETF-spec compliant library, actively maintained, supports JWKS key fetching, key rotation, and all JWT/JWK operations

### 3.3 Refresh Token Security

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

### 3.4 Password Strength Rules

```
Minimum 8 characters
Maximum 128 characters (prevents DoS via extremely long passwords bcrypt-hashing)
Must contain at least one: uppercase letter, lowercase letter, digit, special character
Cannot be the same as the current password (on change-password endpoint)
```

### 3.5 Account Lockout

```
Threshold:   5 consecutive failed login attempts
Lock period: 30 minutes
Counter:     Reset to 0 on any successful login
```

**Timing-safe comparison:** Always run `bcrypt.compare` even for non-existent users (with a dummy hash) to prevent timing attacks that reveal whether an email is registered:
```typescript
// The dummy MUST be a structurally valid bcrypt hash at the same cost factor.
// bcrypt.compare against a malformed string returns almost instantly,
// which re-opens the timing side-channel the dummy exists to close.
// Compute it ONCE at startup:
const DUMMY_HASH = await bcrypt.hash('timing-equalizer-not-a-real-password', 12);

// In the login path:
await bcrypt.compare(password, user?.passwordHash ?? DUMMY_HASH);
// Then check if user actually existed → uniform ~200ms either way
```

### 3.6 Role-Based Access Control (RBAC) & Granular Permissions

Three roles with distinct capabilities:

| Role | Capabilities |
|---|---|
| `CUSTOMER` | Register, login, search flights, create bookings, view own bookings, manage own profile |
| `FLIGHT_ADMIN` | All CUSTOMER permissions + create/update/delete flights and schedules |
| `SUPER_ADMIN` | All permissions + view all users, change user roles, ban/unban accounts, view audit logs |

**How roles are enforced:**
- The JWT `role` claim is set at registration (default `CUSTOMER`) or by `SUPER_ADMIN` at a management endpoint
- The API Gateway injects `X-User-Role` header from the verified JWT
- Each downstream service's route middleware reads `X-User-Role` and rejects requests that don't meet the minimum role
- The User Service's own `/admin` routes enforce the role **again locally** (`requireRole('SUPER_ADMIN')`) as defense in depth — see §7 Layer Rules

**Database seeding:** On first startup, if no `SUPER_ADMIN` exists, the seed script creates one using credentials from environment variables (never hardcoded).

**Granular permissions & scopes inside JWT claims (v3):** instead of hardcoding coarse roles inside downstream microservices, the User Service translates dynamic RBAC relationships at login and injects a dedicated `permissions` string array (scopes) directly into the Access Token claims:

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

*   **Decoupled Verification**: downstream microservices check if the token possesses the specific permission (e.g., `'flights:create'`), completely decoupling route security logic from central user administration. This is additive over the v1 `role`-only model — non-breaking.

### 3.7 Immutable Security Audit Logging (v3)

Critical security operations compile structured history logs into the `audit_logs` table (and optionally a Kafka stream `security-audit-events`) to ensure full compliance auditing:

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

### 3.8 Input Validation Security

- All input validated with Zod before any DB operation (see §5)
- Emails normalized to lowercase + trimmed (prevent case-variant duplicate accounts)
- 6-digit OTPs validated as exactly `^\d{6}$`; passwords enforced against the §3.4 complexity regex
- Raw OTPs and refresh tokens are **never** stored — only their SHA-256 hashes
- Prisma parameterized queries prevent SQL injection; `$queryRaw` uses tagged template literals (also parameterized)
- Request body size capped (`express.json({ limit: '10kb' })`) to blunt oversized-payload DoS

---

## 4. Complete REST API Specification

All endpoints are prefixed with `/api/v1` at the Gateway level. Internally, the User Service listens on `/api/v1/auth` and `/api/v1/admin` routes, plus the public `/.well-known/jwks.json`.

### 4.0 Endpoint Version Scope (BUILD THIS IN ORDER — read before §4)

This section documents the **finished v3 shape**. **Do NOT build all 21 endpoints at once.** Build the **[v1]** set in Phase 2; the **[v3]** set is deferred to Phase 7. Every endpoint below carries a `[v1]`/`[v3]` tag in its heading — honor it.

| # | Endpoint | Scope |
| :-- | :--- | :--- |
| 1 | `POST /auth/register` | **[v1]** |
| 2 | `POST /auth/verify-email` | **[v1]** |
| 3 | `POST /auth/resend-verification` | **[v1]** |
| 4 | `POST /auth/login` | **[v1]** *(v1 path only — MFA branch is [v3], see the v1 response callout)* |
| 5 | `POST /auth/refresh` | **[v1]** |
| 6 | `POST /auth/logout` | **[v1]** |
| 7 | `POST /auth/logout-all` | **[v1]** |
| 8 | `GET /auth/me` | **[v1]** |
| 9 | `PUT /auth/me` | **[v1]** |
| 10 | `POST /auth/change-password` | **[v1]** |
| 11 | `POST /auth/forgot-password` | **[v1]** |
| 12 | `POST /auth/reset-password` | **[v1]** |
| 13 | `GET /auth/sessions` | **[v3]** |
| 14 | `DELETE /auth/sessions/:sessionId` | **[v3]** |
| 15 | `POST /auth/mfa/enable` | **[v3]** |
| 16 | `POST /auth/mfa/verify` | **[v3]** |
| 17 | `POST /auth/mfa/login-verify` | **[v3]** |
| 18 | `GET /admin/users` | **[v3]** |
| 19 | `PUT /admin/users/:userId/roles` | **[v3]** |
| 20 | `GET /.well-known/jwks.json` | **[v1]** |
| 21 | `GET /api/v1/health` | **[v1]** |

> **v1 build set = endpoints 1–12, 20, 21.** Everything that signs a token in v1 signs the **`role` claim only** (from the static `users.role` column) — never a `permissions[]` array (that's v3 dynamic RBAC). v1 login never returns an MFA branch.

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

### Feature 1: User Registration with Email Verification

**Who can call:** No auth required (public)

**Flow:**
1. Client sends `{ name, email, password }` to `POST /api/v1/auth/register`
2. Zod validates: name (min 2 chars), email (valid format), password (min 8 chars, complexity rules)
3. Check if email already exists → 409 Conflict if yes
4. Hash password with `bcrypt(password, 12)` — ~200ms intentionally
5. Generate a 6-digit OTP: `crypto.randomInt(100000, 999999).toString()`
6. Store `SHA-256(code)` in `email_verify_token` with `email_verify_expires_at = NOW() + 10 minutes` — never the raw code
7. In ONE atomic DB transaction:
   - `INSERT INTO users (...)` with `email_verified = false`, `is_active = true` + the `user_profiles` row
   - `INSERT INTO outbox_events (type='USER_REGISTERED', ...)`
8. Email the raw 6-digit code (valid 10 minutes)
9. Return `201 Created` — user must verify email before they can log in

**Why store a hash of the code (not the raw code)?** The DB could be leaked, so the raw code is never stored. But a 6-digit OTP has only 900,000 possibilities — SHA-256 of it is brute-forced offline in milliseconds. The hash is therefore only one layer; the real protections are the **10-minute expiry** and a **verification attempt cap** (max 5 wrong codes → invalidate the code, force a resend).

### Feature 2: Login with Account Lockout Protection

**Who can call:** No auth required (public)

**Flow (order matters — see the anti-enumeration note below):**
1. Zod validates `{ email, password }`
2. Find user by email (B-Tree indexed — sub-millisecond)
3. Check `locked_until` — if set and `locked_until > NOW()` → 423 Locked, return how many seconds remain
4. Run `bcrypt.compare(password, user?.passwordHash ?? DUMMY_HASH)` — ~200ms for existing AND non-existent users alike (§3.5)
5. If password wrong (or user does not exist): return the same generic 401 `UNAUTHORIZED`; for existing users also increment `failed_login_attempts`, and at `>= 5` set `locked_until = NOW() + 30 minutes` and return 423
6. Password correct — **only now** check account state: `is_active = false` → 401 (banned); `email_verified = false` → 401 `EMAIL_NOT_VERIFIED`
7. Proceed with success: reset `failed_login_attempts = 0`, update `last_login_at`, sign RS256 access token (15 min), store `SHA-256(refreshToken)` (7-day expiry, device info, IP), write `USER_LOGGED_IN` to outbox, return `{ accessToken, refreshToken, user }`

**Why are `is_active` / `email_verified` checked AFTER the password compare?** If checked first, an unverified/banned account would return a distinct error instantly — without the ~200ms compare — letting an attacker *without the password* distinguish "registered but unverified" from "not registered" by both content and timing. Post-compare, only someone who already knows the correct password learns account state.

### Feature 3: Access Token Refresh with Token Rotation

**Who can call:** No auth required (public — refresh token is the credential)

**Flow:**
1. Client sends `{ refreshToken }` in request body
2. Compute `SHA-256(refreshToken)` → search `refresh_tokens` by `token_hash`; not found → 401
3. Found → check `expires_at > NOW()` → 401 if expired; fetch user → check `is_active`
4. **Rotation:** in ONE transaction — `DELETE` the found row (the delete is the **atomic guard against concurrent use**: two racing requests both pass the lookup, only one delete wins, the loser gets 401), generate a new raw refresh token, `INSERT` its hash (7-day expiry)
5. Sign a new access token (15-min expiry); return `{ accessToken, refreshToken: <new_token> }`

**Why rotation?** A stolen refresh token works for 7 days silently. With rotation, the attacker's use replaces it; the legitimate user's next use of the now-gone token returns 401, signalling compromise. (v2 reuse detection: revoke ALL of the user's tokens on a replayed rotated-out token.)

### Feature 4: Logout (Single Session & All Sessions)

**Who can call:** Authenticated (Bearer access token)

- **Single-session (`POST /logout`):** extract `jti` from the Gateway-injected `X-User-Jti`/`X-User-Exp` headers, delete the session's refresh token, write `jti` to Redis blacklist with TTL = `exp − now`.
- **All-sessions (`POST /logout-all`):** `DELETE FROM refresh_tokens WHERE user_id = userId`, blacklist current `jti`.

**Why Redis blacklist only stores until token expiry?** After natural expiry the Gateway rejects the token anyway (exp check). Dynamic TTL = `exp − now` lets Redis auto-clean entries; storing them forever would fill Redis with dead keys.

### Feature 5: Email Verification

**Who can call:** No auth required (public)

- **Verify (`POST /verify-email`):** match `SHA-256(code)` against `email_verify_token` AND `email_verify_expires_at > NOW()`; no match/expired → 400 `INVALID_VERIFY_CODE` + increment attempt counter (5 wrong → clear token fields, require resend); match → `email_verified = true`, clear token fields, write `USER_EMAIL_VERIFIED` to outbox.
- **Resend (`POST /resend-verification`):** decoy 200 if not found or already verified; rate-limit 1 request / 2 minutes (else 429); otherwise issue a fresh OTP and reset the attempt counter.

> **Where the per-account resend timer lives (this is distinct from the Gateway's IP rate-limit).** The Gateway limits *by IP* (20 req/15min); the **1-request-per-2-minutes-per-account** limit is a business rule the **User Service enforces itself** using Redis: on each resend, `SET resend:verify:{email} 1 EX 120 NX` — if the key already exists (`NX` fails), return `429`; otherwise the key is set and the OTP is sent. Redis auto-expires the key after 120s. The same pattern (`resend:reset:{email}`) backs the forgot-password resend limit. Using Redis (not a DB timestamp column) keeps this hot, ephemeral counter out of the credential table.

## 4.1 Authentication & Registration Endpoints

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

#### Endpoint 4: POST /api/v1/auth/login **[v1]** *(MFA branch is [v3])*

> **🎯 v1 BUILD TARGET — pin this.** In v1 there is **no MFA** and **no dynamic permissions**. v1 login:
> - signs the access token with the **`role` claim only** (from `users.role`), **never** a `permissions[]` array;
> - **always** succeeds straight to tokens — there is **no `mfaRequired` branch and no `mfaTicket`**;
> - returns the **v1 response shape** below (you may omit the `mfaRequired` field entirely in v1, or hardcode it `false`).
>
> The `mfa_enabled` check, the `MFA_REQUIRED` ticket flow, and `permissions[]` claims described after this callout are **[v3]** — do not build them in Phase 2.

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
    2. Runs timing-safe Bcrypt password comparison (dummy hash for non-existent users — §3.5).
    3. If failed, increments lockout counters (locks at 5) and returns the generic 401.
    4. **[v1]** If successful, checks `is_active` / `email_verified` (post-compare — anti-enumeration, see Feature 2), then signs the RS256 Access Token (with the **`role` claim**) and writes a new SHA-256 refresh token hash to the DB.
    4b. **[v3 only]** After the state checks, if `mfa_enabled = true`:
       - **MFA Active**: Returns a short-lived temporary ticket token and sets `mfaRequired = true`.
       - **No MFA**: Signs the access token (attaching dynamic scopes/permissions in claims) and writes the refresh token hash.
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
          "expiresIn": 900
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

---

### Feature 6: Forgot Password & Password Reset

**Who can call:** No auth required (public)

- **Forgot (`POST /forgot-password`):** **always return `200 OK`** regardless of whether the email exists (anti-enumeration). If found, generate a 6-digit recovery OTP, store `SHA-256(code)` in `reset_token` with `reset_expires_at = NOW() + 10 minutes`, email the raw code (same 2-min resend limit).
- **Reset (`POST /reset-password`):** validate new password complexity; match `SHA-256(code)` against `reset_token` AND `reset_expires_at > NOW()` (same 5-attempt cap); in ONE transaction update `password_hash`, clear reset fields, and `DELETE FROM refresh_tokens WHERE user_id = userId` (log out all devices).

**Why invalidate all sessions on password reset?** If an attacker changed the password, the legitimate user is logged out everywhere and notices. If the legitimate user reset it (suspecting compromise), all the attacker's sessions die.

> **⚠️ Known gap — live access tokens survive a reset (the stateless-JWT problem).** Deleting refresh tokens stops *new* access tokens, but **access tokens already issued stay valid until they expire (≤15 min)** — they're stateless JWTs nobody stores. On a security-sensitive action (reset/change-password) that 15-minute window is a real, if small, exposure. **v1 accepts it** (bounded by the short access-token life). **Hardened option (recommended for v2):** on reset/change-password, also blacklist the user's active access-token `jti`(s) in Redis using the existing logout-blacklist machinery (Feature 4) — e.g. maintain a `user:jtis:{userId}` set, or stamp a `tokensValidFrom` timestamp on the user and reject any access token with `iat < tokensValidFrom` at the Gateway. This fully closes the window.

### Feature 7: Change Password (Authenticated)

**Who can call:** Authenticated (Bearer access token)

**Flow:** extract `userId` from JWT → `bcrypt.compare(currentPassword, passwordHash)` (401 if wrong) → validate `newPassword` complexity → enforce `newPassword !== currentPassword` (400) → hash + update → delete all OTHER refresh tokens (keep current session) → `200 OK`.

### Feature 8: Profile Management

**Who can call:** Authenticated (Bearer access token)

- **Get (`GET /me`):** fetch by `userId` (from `X-User-Id`); return profile only — never `passwordHash` or token fields.
- **Update (`PUT /me`):** only `fullName` is user-editable (email changes require re-verification); validate 2–100 chars; return updated profile.

## 4.2 Profile & Password Management Endpoints

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
*   **Behavior**: Computes SHA-256 of the recovery code, matches it, verifies expiration, and updates the user's password hash in the database. Invalidates all sessions.
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

### Feature 9: Device-Aware Active Session Control (v3)

**Who can call:** Authenticated (Bearer access token)

Users can view, manage, and selectively revoke their active logins from other devices (avoiding full-device lockouts). The `User-Agent` string is parsed into human-readable device/browser metadata, and `isCurrent` is computed as `row.lastJti === req.userJti` — every login/refresh stores the issued access token's `jti` on its refresh-token row (`last_jti` column), the only link between the presented access token and a session row.

## 4.3 Device-Aware Session Management Endpoints (Auth Required)

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

### Feature 10: Time-Based Authenticator MFA (TOTP) (v3)

**Who can call:** Authenticated (Bearer access token); `mfa/login-verify` uses a temporary ticket

Optional security reinforcement using authenticator apps (Google/Microsoft Authenticator):
1. **Enable MFA**: generate a cryptographically random Base32 secret and a provisioning URL `otpauth://totp/SkyHub:user@email.com?secret=SECRET&issuer=SkyHub`.
2. **Verify Setup**: validate the user's initial code via `otplib`; set `mfa_enabled = true`.
3. **Step-Up Auth**: if `mfa_enabled` during login, the service returns `MFA_REQUIRED` + a temporary ticket; the final Access/Refresh tokens are only issued once the client submits its authenticator OTP to `/api/v1/auth/mfa/login-verify`.

## 4.4 Step-Up Multi-Factor Authentication Endpoints (Auth Required)

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

### Feature 11: Administrative RBAC Management (v3)

**Who can call:** `SUPER_ADMIN` only

Administrators list all accounts and reassign roles. These routes are protected by the dynamic RBAC middleware: the Gateway checks the JWT has role `'SUPER_ADMIN'` plus the specific permission, and the service re-checks `requireRole('SUPER_ADMIN')` locally (defense in depth).

## 4.5 Administrative RBAC Endpoints (Requires `SUPER_ADMIN` Role)

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
        "statusCode": 404,
        "name": "NOT_FOUND",
        "message": "The target user ID requested for role updates does not exist."
      },
      "traceId": "tr-adr82xa-81ba"
    }
    ```

---

### Feature 12: JWKS Endpoint & Health Check

- **JWKS (`GET /.well-known/jwks.json`)** — public, no auth. Returns the RS256 public key in JWKS format so any service can verify tokens independently. The Gateway fetches it once on startup, caches it in memory, and refreshes every 24 hours.
- **Health (`GET /api/v1/health`)** — public, no auth. Returns `200` if DB + Redis + Kafka all pass, `503` otherwise. Used by the Load Balancer for readiness probes.

## 4.6 Cluster Metadata & Observability Endpoints

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

#### Endpoint 21: GET /api/v1/health
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

## 5. Zod Validation Schemas

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

## 6. Kafka Event Publishing (Outbox Pattern)

### 6.1 Kafka Topics

**Produced topic:** `user-identity-events`

**Producer:** User Service (the only producer for this topic)

**Consumers:** Search Service (denormalizes user loyalty tier for discount calculation — see `03_Search_Service_Design.md` §7.3)

**Consumed topic:** `booking-events` — the User Service runs a consumer that listens for `BOOKING_COMPLETED` to drive the loyalty tier system (see §6.5).

### 6.2 Standard Message Envelope

```json
{
  "eventId":       "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "eventType":     "USER_REGISTERED",
  "eventVersion":  "1.0",
  "source":        "user-service",
  "correlationId": "req-abc123",
  "timestamp":     "2026-05-28T10:00:00.000Z",
  "payload":       { }
}
```

Kafka message **key = `userId`** → guarantees per-user ordering across partitions.

### 6.3 Event Payloads

| Event Type | Trigger | Payload |
|---|---|---|
| `USER_REGISTERED` | Successful registration | `{ userId, role, loyaltyTier }` |
| `USER_EMAIL_VERIFIED` | Email verification completed | `{ userId }` |
| `USER_LOGGED_IN` | Successful login | `{ userId, loyaltyTier }` |
| `USER_LOYALTY_UPDATED` | Booking count crosses a tier threshold | `{ userId, previousTier, newTier }` |

> `USER_LOGGED_IN` is audit/analytics only — the Search Service deliberately ignores it; it derives the tier from `USER_REGISTERED` / `USER_LOYALTY_UPDATED` (see doc 03 §7.3).

> **⚠️ Design note — write amplification.** Emitting `USER_LOGGED_IN` to the outbox on **every** login means one extra DB row + one Kafka publish per login, for an event **no consumer currently acts on**. At scale (logins are the highest-volume auth event) this is meaningful, no-value write traffic. **Decision for v1:** prefer recording successful logins as an **`audit_logs` insert** (and the existing `last_login_at` update) rather than a cluster-wide event. **Only publish `USER_LOGGED_IN` to Kafka if and when a consumer genuinely needs a real-time login stream** (e.g. a fraud/anomaly detector). Don't pay for a broadcast nobody subscribes to. *(If you keep it as an event for learning purposes, that's fine — just know the trade-off.)*

**All events use the Outbox Pattern:** the event row is written to `outbox_events` in the **same DB transaction** as the business write, then published by the background worker (§6.4). This guarantees at-least-once delivery: a crash after the DB write but before the Kafka publish is recovered on restart.

> ⚠️ **Precision matters (common interview trap):** the outbox pattern is **at-least-once**, *not* exactly-once. The worker can crash *after* publishing but *before* marking the row PUBLISHED — the event is then published again on restart. True exactly-once delivery is impossible across a network; the system achieves **effectively-once processing** by combining at-least-once delivery with **idempotent consumers** (e.g., the Search Service upserts by `userId`). See `01_Architecture.md` §12.6.

### 6.4 Outbox Worker Behaviour

Runs every 5 seconds (recursive `setTimeout`, **not** `setInterval` — a slow tick can never overlap the next and double-claim events). **Behaviour is identical to Flight Service's** (doc 04 §6.4):

1. **Reclaim stranded events:** rows stuck in `PROCESSING` for over 2 minutes (a previous worker crashed mid-publish) are reset back to `PENDING`. At-least-once: a reclaimed event may publish twice; consumers are idempotent by contract.
2. **Claim a batch:** `PENDING → PROCESSING` in one short transaction (`FOR UPDATE SKIP LOCKED`), committed before any network call to free the DB connection.
3. **Publish** each event as a standard envelope, keyed by `userId`.
4. **On success:** mark `PUBLISHED` (+ `published_at`).
5. **On failure:** `retry_count + 1` back to `PENDING`; an event goes `FAILED` only after the retry cap — **never on the first transient failure** (a 30-second Kafka blip must not strand events).

### 6.5 Loyalty Tier System (booking-events Consumer)

**Loyalty tiers drive flight discounts in the Search Service.**

| Tier | Booking Threshold | Discount Applied by Search Service |
|---|---|---|
| SILVER | 0 – 4 completed bookings | 5% |
| GOLD | 5 – 14 completed bookings | 10% |
| PLATINUM | 15+ completed bookings | 15% |

The Booking Service publishes `BOOKING_COMPLETED` when a booking is confirmed. The User Service consumes `booking-events`, increments `user_profiles.booking_count`, recalculates the tier, and — on a tier change — writes `USER_LOYALTY_UPDATED` to the outbox.

**Tier upgrade logic (`loyalty.service.ts`):**
```
calculateTier(bookingCount: number): LoyaltyTier
  bookingCount >= 15  → PLATINUM
  bookingCount >= 5   → GOLD
  default             → SILVER
```

**The consumer is idempotent** — each event's `eventId` is recorded in the `processed_events` table in the **same transaction** as the atomic `{ increment: 1 }`, so an at-least-once redelivery never double-counts (full implementation in §10 Step 7).

---

## 7. Layered Architecture & File Map

```
services/user-service/
│
├── src/
│   │
│   ├── db/                          ← Prisma 7 layout (schema lives inside src/)
│   │   ├── schema.prisma            ← Decoupled IAM: User, UserProfile, Role, Permission, join tables, RefreshToken, OutboxEvent, ProcessedEvent, AuditLog (Section 2.3)
│   │   ├── migrations/              ← Generated by `prisma migrate dev --config src/config/prisma.config.ts`
│   │   │   └── <timestamp>_init/
│   │   │       └── migration.sql
│   │   ├── generated/prisma/        ← Generated client (gitignored)
│   │   └── seed.ts                  ← Creates SUPER_ADMIN + FLIGHT_ADMIN + roles/permissions from env vars
│   │
│   ├── config/
│   │   ├── index.ts                 ← Aggregates configs, re-exports prisma client
│   │   ├── env.ts                   ← Zod-validated env vars — crashes on startup if invalid
│   │   ├── prisma.config.ts         ← Prisma 7 defineConfig: schema/migrations paths + datasource URL
│   │   ├── client.ts                ← Prisma client singleton (pg Pool + @prisma/adapter-pg)
│   │   ├── redis.ts                 ← ioredis client singleton for blacklist writes
│   │   ├── kafka.ts                 ← KafkaJS producer instance
│   │   ├── logger.ts                ← Pino logger with AsyncLocalStorage correlation injection
│   │   └── keys.ts                  ← RSA key pair loading for JWT sign/verify + JWKS export
│   │
│   ├── repositories/
│   │   ├── user.repository.ts       ← All Prisma user queries — NO business logic here
│   │   ├── token.repository.ts      ← All Prisma refresh_token queries
│   │   └── outbox.repository.ts     ← Insert + update outbox_events
│   │
│   ├── services/
│   │   ├── auth.service.ts          ← Registration, login, logout, email verification logic
│   │   ├── token.service.ts         ← JWT sign/verify, refresh token create/rotate/delete
│   │   ├── loyalty.service.ts       ← Tier calculation, upgrade detection (idempotent consumer)
│   │   └── email.service.ts         ← nodemailer wrapper for verification + reset emails
│   │
│   ├── controllers/
│   │   └── auth.controller.ts       ← HTTP layer only: parse req, call service, send res
│   │
│   ├── routes/
│   │   ├── auth.routes.ts           ← Maps HTTP verbs + paths → controller methods
│   │   ├── health.routes.ts         ← GET /api/v1/health — DB + Redis + Kafka liveness checks
│   │   ├── jwks.routes.ts           ← GET /.well-known/jwks.json — public key distribution
│   │   ├── metrics.routes.ts        ← GET /metrics — Prometheus scrape endpoint
│   │   └── schemas/
│   │       └── auth.schemas.ts      ← All Zod schemas (from Section 5)
│   │
│   ├── middlewares/
│   │   ├── validate.ts              ← Generic Zod validation middleware factory
│   │   ├── requireAuth.ts           ← Reads X-User-Id/Role/Jti/Exp headers (injected by Gateway)
│   │   ├── requireRole.ts           ← Local role enforcement on /admin routes (defense in depth)
│   │   └── errorHandler.ts          ← Re-exports globalErrorHandler + notFoundHandler from common-utils
│   │
│   ├── events/
│   │   ├── producers/
│   │   │   └── user.producer.ts     ← KafkaJS publish function for user-identity-events
│   │   ├── consumers/
│   │   │   └── booking.consumer.ts  ← Listens to booking-events → increments booking_count
│   │   └── outbox.worker.ts         ← Polls outbox_events every 5s, publishes to Kafka
│   │
│   ├── types/
│   │   ├── express.d.ts             ← Augments Express Request: req.userId, req.userRole, req.userJti
│   │   └── jwt.types.ts             ← JwtPayload interface (sub, role, loyaltyTier, jti, iat, exp)
│   │
│   ├── utils/
│   │   ├── crypto.utils.ts          ← hashToken(), generateRawToken(), generateJti()
│   │   └── response.utils.ts        ← sendSuccess(), sendError() helpers
│   │
│   ├── app.ts                       ← Express setup: helmet, cors, body-parser, routes
│   └── server.ts                    ← Boot: DB connect, Redis connect, Kafka connect, listen
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

**Defense-in-depth on `/admin`:** the Gateway already checks the role, but the service enforces it AGAIN locally via `requireRole('SUPER_ADMIN')`. `requireAuth` alone would let any logged-in CUSTOMER through if the Gateway check ever regressed.

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
    "build":        "tsup src/server.ts --format esm --clean --minify",
    "start":        "node dist/server.js",
    "db:migrate":   "prisma migrate dev --config src/config/prisma.config.ts",
    "db:deploy":    "prisma migrate deploy --config src/config/prisma.config.ts",
    "db:generate":  "prisma generate --config src/config/prisma.config.ts",
    "db:studio":    "prisma studio --config src/config/prisma.config.ts",
    "seed":         "tsx src/db/seed.ts",
    "lint":         "eslint .",
    "test":         "vitest",
    "test:coverage":"vitest run --coverage",
    "typecheck":    "tsc --noEmit"
  },
  "dependencies": {
    "@prisma/adapter-pg":   "^7.8.0",
    "@prisma/client":       "^7.8.0",
    "@skyhub/common-utils": "*",
    "@skyhub/shared-types": "*",
    "bcrypt":               "^5.1.1",
    "cors":                 "^2.8.5",
    "dotenv":               "^17.4.2",
    "express":              "^5.2.1",
    "helmet":               "^7.1.0",
    "ioredis":              "^5.3.2",
    "jose":                 "^5.3.0",
    "kafkajs":              "^2.2.4",
    "nodemailer":           "^6.9.13",
    "pg":                   "^8.21.0",
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
    "@types/pg":            "^8.20.0",
    "@types/supertest":     "^6.0.2",
    "@vitest/coverage-v8":  "^1.6.0",
    "pino-pretty":          "^11.0.0",
    "prisma":               "^7.8.0",
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
| `@prisma/client` | Type-safe PostgreSQL ORM (v7) — generates a TypeScript client from your schema (modern `prisma-client` generator, imported from `src/db/generated/prisma/client`). |
| `@prisma/adapter-pg` + `pg` | Prisma 7 driver adapter — the client is constructed over an explicit `pg.Pool` (`new PrismaClient({ adapter })`), giving direct control of pool size/timeouts. |
| `bcrypt` | Adaptive password hashing with built-in salt generation |
| `cors` | Express CORS middleware — needed so internal services can configure cross-origin policy |
| `dotenv` | Loads `.env` file into `process.env` before the Zod env schema runs |
| `express` | HTTP server framework (v5 — async errors propagate to error middleware natively, no patch needed) |
| `helmet` | Sets 7 security HTTP headers in one line |
| `ioredis` | Redis client for the JWT `jti` blacklist (DB 0, shared with the Gateway) |
| `jose` | Modern JWT library with RS256 / JWKS support (replaces `jsonwebtoken` for RS256) |
| `kafkajs` | Official Kafka Node.js client — producer (`user-identity-events`) + consumer (`booking-events`) |
| `nodemailer` | SMTP transport for verification + password-reset emails |
| `pino` + `pino-http` | Structured JSON logger — 5× faster than Winston, native `child()` for per-request context |
| `prom-client` | Prometheus metrics exporter — powers the `/metrics` endpoint |
| `pino-pretty` | Dev-only: pipes Pino JSON output into human-readable format (`npm run dev \| npx pino-pretty`) |
| `tsup` | Fast esbuild-based bundler for the production `build` (matches flight-service) |
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
# Read by src/config/prisma.config.ts (datasource URL) and src/config/client.ts (pg Pool).
# connection_limit: max pool connections (keep ≤ 10 for local dev)
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
# Must be PKCS#8 ("BEGIN PRIVATE KEY", from `openssl genpkey`) — jose's importPKCS8() rejects PKCS#1 ("BEGIN RSA PRIVATE KEY")
JWT_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"
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

  // ── Seed credentials (only read by src/db/seed.ts, not at runtime) ───
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
3. Create the Prisma 7 layout by hand (no `npx prisma init` — that scaffolds the legacy `prisma/` folder):
   - `src/db/schema.prisma` ← schema from Section 2.3
   - `src/config/prisma.config.ts` ← `defineConfig` (datasource URL + schema/migrations paths)
4. Create `src/config/env.ts` (Section 9 code) — this must be the very first file so everything else can import it
5. Create all other config files from Step 3: `client.ts`, `redis.ts`, `kafka.ts`, `logger.ts`, `keys.ts`
6. Generate RSA key pair (run once, store the output in `.env`):
   ```bash
   # PKCS#8 format — required by jose's importPKCS8() in keys.ts
   openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out private.pem
   openssl rsa -in private.pem -pubout -out public.pem
   # Then paste the contents into .env as JWT_PRIVATE_KEY and JWT_PUBLIC_KEY
   # Replace actual newlines with \n so they fit on one line in .env
   ```
7. Copy `.env.example` to `.env` and fill in all values (use Mailtrap for SMTP in dev)

**`tsconfig.json` for user-service:**
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

**`src/config/prisma.config.ts`:**
```typescript
import { defineConfig } from 'prisma/config';
import path from 'node:path';
import 'dotenv/config';

export default defineConfig({
  schema: path.join('src', 'db', 'schema.prisma'),
  migrations: { path: path.join('src', 'db', 'migrations') },
  datasource: { url: process.env.DATABASE_URL },
});
```

**Validation:** Run `npm run typecheck` from `services/user-service/`. Zero errors. Start the service with `npm run dev` — if any env var is missing it should crash immediately with a clear field-by-field error list from Zod.

---

### Step 2: Database Migration

**What to do:**
1. Start Docker infrastructure from the monorepo root: `docker compose up -d`
2. Verify Postgres is running: `docker compose ps`
3. Run initial migration (wraps `prisma migrate dev --config src/config/prisma.config.ts`):
   ```bash
   cd services/user-service
   npm run db:migrate -- --name init
   ```
4. (Optional) Replace the default outbox compound index with the partial index from Section 2.4 in the generated `migration.sql`.
5. Verify tables: `npm run db:studio` — confirm `users`, `user_profiles`, `refresh_tokens`, `outbox_events`, `processed_events` (and the RBAC/audit tables) exist with all columns
6. Create `src/db/seed.ts`:

```typescript
import 'dotenv/config';   // seed.ts runs standalone — must load .env itself
import { PrismaClient } from './generated/prisma/client.js';
import { PrismaPg } from '@prisma/adapter-pg';
import { Pool } from 'pg';
import bcrypt from 'bcrypt';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });

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
      role:  'SUPER_ADMIN' as const,   // v1 static column — what the JWT is signed from
      roleId: superAdminRole.id,        // v3 dynamic RBAC link
    },
    {
      name:  process.env.FLIGHT_ADMIN_NAME  ?? 'Flight Admin',
      email: process.env.FLIGHT_ADMIN_EMAIL ?? 'flightadmin@skyhub.com',
      pass:  process.env.FLIGHT_ADMIN_PASSWORD ?? 'FlightAdmin1!@#',
      role:  'FLIGHT_ADMIN' as const,
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
          role: admin.role,        // v1 static role
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

**Validation:** Run `npm run seed`. Open `npm run db:studio` — two users with `email_verified = true` should exist.

---

### Step 3: Utilities & Common Infrastructure

**What to do — create these files:**

**`src/config/client.ts`:**
```typescript
import { PrismaClient } from '../db/generated/prisma/client.js';
import { PrismaPg } from '@prisma/adapter-pg';
import { Pool } from 'pg';
import { env } from './env.js';

const pool = new Pool({ connectionString: env.DATABASE_URL });
const adapter = new PrismaPg(pool);

const globalForPrisma = globalThis as unknown as { prisma?: PrismaClient };

export const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    adapter,
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

// Real connectivity flag for /health — kafkajs has no isConnected() method
let kafkaUp = false;
kafkaProducer.on('producer.connect',    () => { kafkaUp = true;  });
kafkaProducer.on('producer.disconnect', () => { kafkaUp = false; });
export const isKafkaConnected = () => kafkaUp;
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

> **⚠️ Production note — don't block the request on SMTP.** Below, `sendMail` is **awaited inside the request path**. If SMTP is slow or down, **registration / forgot-password slow down or fail** — you've made a user-facing endpoint depend on a flaky external system. This is acceptable for **v1 + local dev (Mailtrap)** only.
>
> **Production-grade pattern (the SkyHub architecture already supports it):** decouple email from the request. Two valid approaches:
> 1. **Outbox/event-driven (preferred, consistent with §6):** the register/forgot transaction emits the event; the **Notification Service** (BullMQ worker, per `01_Architecture.md`) consumes it and sends the email out-of-band. The API returns `201`/`200` immediately, independent of SMTP.
> 2. **Fire-and-forget / queued in-process:** push the send onto a BullMQ job and return; a worker retries on failure with backoff.
>
> Either way: **never let the HTTP response wait on email delivery, and never fail a registration because an email bounced.** The OTP is in the DB; resend covers a lost email. Keep the inline version below for v1, but migrate to the queue in the Phase-7 hardening pass.

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
  name:       string;     // machine-readable error code — matches the §4 envelope + common-utils handler
  message:    string;
  details?:   Array<{ field: string; message: string }>;
  traceId:    string;
}

export function sendSuccess({ res, statusCode, message, data = null, traceId }: SuccessOptions) {
  res.status(statusCode).json({ success: true, message, data, traceId });
}

export function sendError({ res, statusCode, name, message, details, traceId }: ErrorOptions) {
  res.status(statusCode).json({ success: false, error: { statusCode, name, message, details }, traceId });
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
        res, statusCode: 400, name: 'VALIDATION_ERROR',
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
  const userExp  = req.headers['x-user-exp']  as string;  // token exp (unix secs) — blacklist TTL = exp − now

  if (!userId || !userRole) {
    return sendError({
      res, statusCode: 401, name: 'UNAUTHORIZED',
      message: 'Authentication required',
      traceId: req.headers['x-correlation-id'] as string ?? '',
    });
  }

  req.userId   = userId;
  req.userRole = userRole;
  req.userJti  = userJti;
  req.userExp  = userExp ? Number(userExp) : undefined;
  next();
}
```

**`src/middlewares/requireRole.ts`:**
```typescript
export function requireRole(...allowed: string[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.userRole || !allowed.includes(req.userRole)) {
      return sendError({
        res, statusCode: 403, name: 'FORBIDDEN',
        message: 'Insufficient permissions',
        traceId: req.headers['x-correlation-id'] as string ?? '',
      });
    }
    next();
  };
}
```

**`src/middlewares/errorHandler.ts`** — `AppError` class & global handler are imported and re-exported from the compiled shared package `@skyhub/common-utils`:
```typescript
export { globalErrorHandler, notFoundHandler } from '@skyhub/common-utils';
```

**`src/types/express.d.ts`:**
```typescript
declare namespace Express {
  interface Request {
    userId?:   string;
    userRole?: string;
    userJti?:  string;    // JWT ID — injected by Gateway, used for blacklisting on logout
    userExp?:  number;    // JWT exp (unix seconds) — blacklist TTL = exp − now
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
create(tx, eventType, payload): Promise<void>            // accepts Prisma tx — written atomically with the business change
reclaimStale(): Promise<number>                          // PROCESSING with updated_at older than 2 min → back to PENDING
claimPending(limit: number): Promise<OutboxEvent[]>      // FOR UPDATE SKIP LOCKED → mark PROCESSING → commit (short tx)
markPublished(id: string): Promise<void>
retryOrFail(id: string, maxRetries: number): Promise<void> // retry_count+1 → PENDING; FAILED only past the cap
```

**Rule:** Every method in a repository is a simple Prisma call. No if/else. No calculations. Just SQL. This is the only layer that imports `prisma` (from `../config/client.js`).

**Validation:** Write a quick test script that calls `userRepository.create(...)` and logs the result. Check `npm run db:studio` to confirm the row exists.

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
    ← THE DELETE IS THE ATOMIC GUARD: two concurrent requests with the same token
      both pass findByHash, but only one delete succeeds — Prisma throws P2025 for
      the loser. Catch it and map to the same 401 (the token was already rotated,
      by a parallel tab or by a thief replaying it).
    ← v2 upgrade (reuse detection): on that P2025/401, revoke ALL of the user's
      refresh tokens — a replayed rotated-out token is the signature of theft.
  → return createRefreshToken + signAccessToken for existing.userId
    (store the new access token's jti on the new row's last_jti — powers GET /sessions isCurrent)
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
  → check locked_until (block if locked)
  → bcrypt.compare(password, user.passwordHash)
  → handle wrong password (increment counter, lock for 30 minutes on 5 consecutive failures)
  → POST-compare: check is_active, email_verified (anti-enumeration)
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
  → update password hash in credentials database, clear reset tokens, delete all refresh tokens

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

// Kafka is at-least-once — this handler MUST be idempotent and race-safe:
//  • processed_events PK eats replayed events (dedupe in the SAME transaction)
//  • { increment: 1 } is atomic — no read-modify-write lost update between
//    two concurrent BOOKING_COMPLETED events for the same user
//  • bookingCount/loyaltyTier live on UserProfile, not User
async handleBookingCompleted(eventId: string, userId: string): Promise<void> {
  await prisma.$transaction(async (tx) => {
    try {
      await tx.processedEvent.create({ data: { eventId } });   // idempotency guard
    } catch (e) {
      if (isUniqueViolation(e)) return;                        // replay — no-op
      throw e;
    }

    const profile = await tx.userProfile.update({
      where: { userId },
      data:  { bookingCount: { increment: 1 } },
    });

    const newTier = this.calculateTier(profile.bookingCount);
    if (newTier !== profile.loyaltyTier) {
      await tx.userProfile.update({ where: { userId }, data: { loyaltyTier: newTier } });
      await outboxRepository.create(tx, 'USER_LOYALTY_UPDATED', {
        userId, previousTier: profile.loyaltyTier, newTier,
      });
    }
  });
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
import { requireRole } from '../middlewares/requireRole';
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

export { router as authRouter };

// ─── 5. ADMINISTRATIVE CONTROL ROUTING — separate router, mounted at /api/v1/admin ──
// Defense in depth: the Gateway already checks the role, but the service enforces it
// AGAIN locally — requireAuth alone would let any logged-in CUSTOMER through if the
// Gateway check ever regressed.
const adminRouter = Router();
adminRouter.get('/users',               requireAuth, requireRole('SUPER_ADMIN'), authController.adminListUsers);
adminRouter.put('/users/:userId/roles', requireAuth, requireRole('SUPER_ADMIN'), validate(schemas.AdminUpdateRolesSchema), authController.adminUpdateUserRoles);

export { adminRouter };
```

**`src/app.ts`:**
```typescript
// Note: no 'express-async-errors' needed — Express 5 propagates async errors natively
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import { pinoHttp } from 'pino-http';
import { authRouter, adminRouter } from './routes/auth.routes';
import { healthRouter } from './routes/health.routes';
import { jwksRouter } from './routes/jwks.routes';
import { metricsRouter } from './routes/metrics.routes';
import { globalErrorHandler, notFoundHandler } from './middlewares/errorHandler';
import { logger } from './config/logger';

const app = express();

app.use(helmet());
app.use(cors({ origin: false }));     // No direct browser access — Gateway handles CORS
app.use(pinoHttp({ logger }));

app.use(express.json({ limit: '10kb' }));  // Limit request body size

// Routes
app.use('/api/v1/auth', authRouter);
app.use('/api/v1/admin', adminRouter);
app.use('/.well-known', jwksRouter);
app.use('/api/v1', healthRouter);   // GET /api/v1/health (cluster convention)
app.use('/', metricsRouter);        // GET /metrics — Prometheus scrape endpoint

// 404 fallback — after all routes, before the error handler (from common-utils)
app.use(notFoundHandler);
// Global error handler — must be last (4-arg)
app.use(globalErrorHandler);

export { app };
```

**`src/server.ts`:**
```typescript
import { app } from './app.js';
import { env } from './config/env.js';
import { prisma } from './config/client.js';
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
    // Force-exit backstop — a hung connection must never block shutdown forever
    setTimeout(() => {
      logger.error('Forced shutdown after 10s timeout');
      process.exit(1);
    }, 10_000).unref();
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
import { prisma } from '../config/client.js';
import { redisClient } from '../config/redis.js';
import { isKafkaConnected } from '../config/kafka.js';

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

  checks.kafka = isKafkaConnected() ? 'ok' : 'error: not connected';

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

**Validation:** `npm run dev` — server starts, logs `User service listening`. Hit `GET http://localhost:3001/api/v1/health` → `{ status: "healthy" }`. Hit `GET http://localhost:3001/metrics` → Prometheus text. Hit `POST http://localhost:3001/api/v1/auth/register` with valid body → `201 Created`.

---

### Step 7: Outbox Worker & Kafka Events

**`src/events/outbox.worker.ts`** — same design as Flight Service (doc 04 §6.4):
```typescript
import { outboxRepository } from '../repositories/outbox.repository';
import { userProducer } from './producers/user.producer';
import { logger } from '../config/logger';

const POLL_MS = 5000;
const MAX_RETRIES = 10;

// Recursive setTimeout, NOT setInterval — a slow tick (Kafka backpressure)
// can never overlap the next one and double-claim the same events.
export function startOutboxWorker(): void {
  const tick = async () => {
    try {
      // 1. Reclaim events stranded in PROCESSING (a previous worker crashed mid-publish)
      await outboxRepository.reclaimStale();

      // 2. Claim a batch: PENDING → PROCESSING in one short transaction
      //    (FOR UPDATE SKIP LOCKED — commit before any network call)
      const batch = await outboxRepository.claimPending(100);

      for (const event of batch) {
        try {
          await userProducer.publish(event.eventType, event.payload);
          await outboxRepository.markPublished(event.id);
        } catch (err) {
          logger.error({ eventId: event.id, err }, 'Publish failed — will retry');
          // Back to PENDING with retry_count+1; FAILED only past MAX_RETRIES.
          await outboxRepository.retryOrFail(event.id, MAX_RETRIES);
        }
      }
    } catch (err) {
      logger.error(err, 'Outbox worker tick error');
    } finally {
      setTimeout(tick, POLL_MS);
    }
  };
  setTimeout(tick, POLL_MS);
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

      // Deliberately NO try/catch here: if the handler throws, kafkajs does NOT
      // commit the offset and redelivers the message — swallowing the error would
      // commit past a lost event (at-most-once). Replays are safe because the
      // handler dedupes by eventId (processed_events PK).
      await loyaltyService.handleBookingCompleted(event.eventId, event.payload.userId);
      logger.info({ userId: event.payload.userId }, 'Booking count updated');
    },
  });
}
```

**Validation:**
1. Start Kafka: `docker compose up -d kafka`
2. Start the service: `npm run dev`
3. In a second terminal, use `kafkajs` or `kcat` to produce a test event to `booking-events`
4. Watch the service logs — it should process the event and update `booking_count` in DB
5. After updating booking_count to 5: check `npm run db:studio` — `loyalty_tier` should change to `GOLD`
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
| **Loyalty upgrades** | Upgrades SILVER → GOLD at 5 bookings, and GOLD → PLATINUM at 15 bookings via Kafka listeners, writing outbox upgrade events. |

---

## Quick Reference: Build Checklist

```
Step 1  ✓  package.json, tsconfig.json, src/db/schema.prisma + src/config/prisma.config.ts, .env created
Step 2  ✓  prisma migrate dev run (db:migrate), Dynamic RBAC, User & Profile tables seeded (src/db/seed.ts)
Step 3  ✓  Singletons (client.ts, Redis, Kafka, logger, keys), middlewares, compiled common-utils errors & envelopes
Step 4  ✓  User, Token, Outbox repositories implemented and tested
Step 5  ✓  AuthService, TokenService, LoyaltyService with dynamic JWT scopes and 6-digit OTPs
Step 6  ✓  Controllers, Routes (all 21 endpoints mapped), app.ts, server.ts boots, /health ok
Step 7  ✓  OutboxWorker running, Kafka event listener processing upgrades
Step 8  ✓  JWKS endpoint returning public key in JWK format
Step 9  ✓  Integration tests pass for all validation checklist items
```

Once all 9 steps pass their validation, the User Service v1 is complete. Per [`00_Build_Roadmap.md`](00_Build_Roadmap.md) this is **Phase 2** (together with the API Gateway). Next up: Phase 3, the Search Service.

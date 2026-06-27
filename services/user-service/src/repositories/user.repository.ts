import { InternalServerError } from "@skyhub/common-utils";
import { prisma } from "../config";
import { mapPrismaError } from "../db/prisma-error.mapper";
import { CreateUserDTO, AuthUser, UserVerificationView, UserAuthView } from "../dto/auth.dto";

// Lockout policy — kept here because the lock is WRITTEN atomically with the
// failed-attempt increment (read-then-write must not straddle the layer boundary).
const MAX_FAILED_LOGINS = 5;
const LOCK_DURATION_MS = 15 * 60 * 1000; // 15-minute lockout window

export async function createUser(input: CreateUserDTO): Promise<AuthUser> {
    try {
        // Nested write → User + UserProfile are created atomically in ONE transaction.
        // role / isActive / emailVerified come from schema @default — the client never sets them.
        // include the profile so we can return what the DB actually persisted.
        const user = await prisma.user.create({
            data: {
                email: input.email,
                passwordHash: input.passwordHash,
                emailVerifyToken: input.emailVerifyToken,
                emailVerifyExpiresAt: input.emailVerifyExpiresAt,
                profile: {
                    create: { fullName: input.fullName },
                },
            },
            include: { profile: true },
        });

        // The nested create guarantees a profile exists; `profile` is only typed
        // nullable because the schema relation is optional. If it's ever missing,
        // that's a broken invariant — fail loud, never fabricate from input.
        if (!user.profile) {
            throw new InternalServerError("User was created without a profile");
        }

        // Domain shape only — every field reflects what was STORED; passwordHash never leaves.
        return {
            id: user.id,
            email: user.email,
            fullName: user.profile.fullName,
            emailVerified: user.emailVerified,
        };
    } catch (err) {
        throw mapPrismaError(err); // P2002 (duplicate email) → 409 Conflict, automatically
    }
}

// Reads the verify-relevant fields by email. Read → null (no throw) if not found.
export async function findUserForVerification(email: string): Promise<UserVerificationView | null> {
    try {
        return await prisma.user.findUnique({
            where: { email },
            select: {
                id: true,
                emailVerified: true,
                emailVerifyToken: true,
                emailVerifyExpiresAt: true,
                emailVerifyAttempts: true,
            },
        });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Success: verified + clear the token (single-use) + reset the attempt counter.
export async function markEmailVerified(userId: string): Promise<void> {
    try {
        await prisma.user.update({
            where: { id: userId },
            data: {
                emailVerified: true,
                emailVerifyToken: null,
                emailVerifyExpiresAt: null,
                emailVerifyAttempts: 0,
            },
        });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Atomic increment of the wrong-guess counter; returns the new count.
export async function bumpVerifyAttempts(userId: string): Promise<number> {
    try {
        const user = await prisma.user.update({
            where: { id: userId },
            data: { emailVerifyAttempts: { increment: 1 } },
            select: { emailVerifyAttempts: true },
        });
        return user.emailVerifyAttempts;
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Invalidate the pending code (used when the attempt cap is hit → forces a resend).
export async function clearVerifyToken(userId: string): Promise<void> {
    try {
        await prisma.user.update({
            where: { id: userId },
            data: { emailVerifyToken: null, emailVerifyExpiresAt: null },
        });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Issue a fresh OTP (resend): set a new hash + expiry and reset the attempt counter.
export async function setEmailVerifyToken(userId: string, token: string, expiresAt: Date): Promise<void> {
    try {
        await prisma.user.update({
            where: { id: userId },
            data: {
                emailVerifyToken: token,
                emailVerifyExpiresAt: expiresAt,
                emailVerifyAttempts: 0,
            },
        });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Reads the credential + lockout + profile fields by email. Read → null (no throw) if not found.
// ⚠️ Returns passwordHash + lockout fields — INTERNAL only; the service must never leak them.
export async function findUserForLogin(email: string): Promise<UserAuthView | null> {
    try {
        const user = await prisma.user.findUnique({
            where: { email },
            select: {
                id: true,
                email: true,
                passwordHash: true,
                role: true,
                isActive: true,
                emailVerified: true,
                failedLoginAttempts: true,
                lockedUntil: true,
                profile: { select: { fullName: true, loyaltyTier: true } },
            },
        });

        if (!user) return null;

        // Invariant: every user has a profile (nested-created at register). Fail loud if not.
        if (!user.profile) {
            throw new InternalServerError("User exists without a profile");
        }

        return {
            id: user.id,
            email: user.email,
            passwordHash: user.passwordHash,
            role: user.role,
            isActive: user.isActive,
            emailVerified: user.emailVerified,
            failedLoginAttempts: user.failedLoginAttempts,
            lockedUntil: user.lockedUntil,
            fullName: user.profile.fullName,
            loyaltyTier: user.profile.loyaltyTier,
        };
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Atomic increment of the failure counter; locks the account once the cap is hit.
export async function recordFailedLogin(userId: string): Promise<void> {
    try {
        const user = await prisma.user.update({
            where: { id: userId },
            data: { failedLoginAttempts: { increment: 1 } },
            select: { failedLoginAttempts: true },
        });

        if (user.failedLoginAttempts >= MAX_FAILED_LOGINS) {
            await prisma.user.update({
                where: { id: userId },
                data: { lockedUntil: new Date(Date.now() + LOCK_DURATION_MS) },
            });
        }
    } catch (err) {
        throw mapPrismaError(err);
    }
}

// Successful login clears the slate: reset failures, unlock, stamp lastLoginAt.
export async function recordSuccessfulLogin(userId: string): Promise<void> {
    try {
        await prisma.user.update({
            where: { id: userId },
            data: {
                failedLoginAttempts: 0,
                lockedUntil: null,
                lastLoginAt: new Date(),
            },
        });
    } catch (err) {
        throw mapPrismaError(err);
    }
}
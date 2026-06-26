import { InternalServerError } from "@skyhub/common-utils";
import { prisma } from "../config";
import { mapPrismaError } from "../db/prisma-error.mapper";
import { CreateUserDTO, AuthUser, UserVerificationView } from "../dto/auth.dto";

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
import { z } from "zod";
import { registerSchema, verifyEmailSchema, resendVerificationSchema } from "../validators/auth.validator";

// (a) what the SERVICE receives — raw request shapes derived from Zod. No Prisma.
export type RegisterDTO = z.infer<typeof registerSchema>;
export type VerifyEmailDTO = z.infer<typeof verifyEmailSchema>;
export type ResendVerificationDTO = z.infer<typeof resendVerificationSchema>;

// what findUserForVerification returns — the verify-relevant fields only (internal use).
export type UserVerificationView = {
  id: string;
  emailVerified: boolean;
  emailVerifyToken: string | null;
  emailVerifyExpiresAt: Date | null;
  emailVerifyAttempts: number;
};

// (a.2) what the REPOSITORY receives — post-hashing. The raw password never reaches
//       the repo; the service hashes it, renames `name` → `fullName`, and computes
//       the OTP hash + expiry first. Stored atomically with the user.
export type CreateUserDTO = {
  email: string;
  passwordHash: string;
  fullName: string;
  emailVerifyToken: string;     // hashOtp(otp) — never the raw code
  emailVerifyExpiresAt: Date;   // now + 10 min
};

// (b) the domain user the repo RETURNS — owned by us, NOT Prisma's model.
//     Keeps Prisma sealed inside the repository (ORM-swappable). Note: NO
//     passwordHash here — credential material never leaves the repository.
export type AuthUser = {
  id: string;
  email: string;
  fullName: string;
  emailVerified: boolean;
};

// (c) what the CLIENT sees after registration — domain user, `id` → `userId`,
//     and absolutely no credential fields.
export type RegisterResponseDTO = {
  userId: string;
  email: string;
  name: string;
  emailVerified: boolean;
};

/**
 * Single source of truth for "domain AuthUser → client register response".
 * Builds a NEW object with an explicit allowlist, so internal fields can never
 * leak even if the domain type grows later.
 */
export function toRegisterResponse(user: AuthUser): RegisterResponseDTO {
  return {
    userId: user.id,
    email: user.email,
    name: user.fullName,
    emailVerified: user.emailVerified,
  };
}
import { z } from "zod";
import { registerSchema, verifyEmailSchema, resendVerificationSchema, loginSchema } from "../validators/auth.validator";

// (a) what the SERVICE receives — raw request shapes derived from Zod. No Prisma.
export type RegisterDTO = z.infer<typeof registerSchema>;
export type VerifyEmailDTO = z.infer<typeof verifyEmailSchema>;
export type ResendVerificationDTO = z.infer<typeof resendVerificationSchema>;
export type LoginDTO = z.infer<typeof loginSchema>;

// ORM-agnostic role union (mirrors the Prisma AccountRole enum values) — keeps the
// DTO decoupled from Prisma. Goes into the JWT `role` claim.
export type AccountRole = "CUSTOMER" | "FLIGHT_ADMIN" | "SUPER_ADMIN";

// what findUserForVerification returns — the verify-relevant fields only (internal use).
export type UserVerificationView = {
  id: string;
  emailVerified: boolean;
  emailVerifyToken: string | null;
  emailVerifyExpiresAt: Date | null;
  emailVerifyAttempts: number;
};

// what findUserForLogin returns — credential + lockout + profile fields the SERVICE
// needs internally. ⚠️ passwordHash / lockout fields must NEVER reach the response.
export type UserAuthView = {
  id: string;
  email: string;
  passwordHash: string;
  role: AccountRole;
  isActive: boolean;
  emailVerified: boolean;
  failedLoginAttempts: number;
  lockedUntil: Date | null;
  fullName: string;        // from profile (for the response)
  loyaltyTier: string;     // from profile (for the response)
};

// what the CLIENT sees on successful login — clean allowlist, NO credential/lockout.
// refreshToken is added to `tokens` in AUTH 3 (refresh + rotation).
export type LoginResponseDTO = {
  user: { userId: string; email: string; fullName: string; loyaltyTier: string };
  tokens: { accessToken: string; expiresIn: number };
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
import { z } from "zod";

// Password policy (§3.4): length-bounded + complexity.
// max 128 is a DoS guard — bcrypt is deliberately slow, so we cap how much it hashes.
const passwordSchema = z
  .string()
  .min(8, "Password must be at least 8 characters")
  .max(128, "Password must not exceed 128 characters")
  .regex(
    /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9])/,
    "Password must contain an uppercase letter, a lowercase letter, a number, and a special character",
  );

// Normalize FIRST (trim + lowercase) so the DB @unique catches case-variant
// duplicates (John@x.com vs john@x.com), THEN validate format via z.email()
// (Zod v4 — the old `.email()` string method is deprecated).
const emailSchema = z.string().trim().toLowerCase().pipe(z.email("Invalid email format"));

// 6-digit numeric OTP.
const otpCodeSchema = z
  .string()
  .trim()
  .length(6, "Verification code must be exactly 6 digits")
  .regex(/^\d{6}$/, "Verification code must contain only digits");

export const registerSchema = z.object({
  name: z
    .string()
    .trim()
    .min(2, "Name must be at least 2 characters")
    .max(100, "Name must be at most 100 characters"),
  email: emailSchema,
  password: passwordSchema,
});

export const verifyEmailSchema = z.object({
  email: emailSchema,
  code: otpCodeSchema,
});

export const resendVerificationSchema = z.object({
  email: emailSchema,
});
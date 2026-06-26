import crypto from "crypto";

/**
 * Generates a cryptographically secure 6-digit OTP (100000–999999).
 * Uses crypto.randomInt (CSPRNG, unpredictable) — NOT Math.random() (predictable PRNG).
 * Starting at 100000 guarantees exactly 6 digits (no leading-zero/short codes).
 */
export function generateOtp(): string {
  return crypto.randomInt(100000, 1000000).toString();
}

/**
 * SHA-256 hash of an OTP → 64-char hex. Fast + DETERMINISTIC so verify can compare
 * hashes directly (hashOtp(submitted) === storedHash). No salt: we need determinism,
 * and the real defenses for a low-entropy code are the 10-min expiry + 5-attempt cap,
 * not the hash. The hash only keeps the raw code out of the DB (leak defense).
 */
export function hashOtp(code: string): string {
  return crypto.createHash("sha256").update(code).digest("hex");
}
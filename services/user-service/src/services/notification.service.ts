import { logger } from "@skyhub/common-utils";

/**
 * Delivers a verification code to the user — the stable SEAM between identity
 * logic and delivery. `async` by design so the real implementation (outbox row /
 * BullMQ job / SMTP) can slot in without changing a single caller.
 *
 * DEV STUB: logs the raw code so you can read it during development.
 * ⚠️ Replace with the real Notification Service before production — OTPs must
 * never be logged in a real deployment.
 */
export async function sendVerificationCode(email: string, code: string): Promise<void> {
  logger.info({ data: { email, code } }, "[DEV] verification code — replace with Notification Service");
}
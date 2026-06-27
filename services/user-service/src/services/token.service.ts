import { SignJWT } from "jose";
import { randomUUID } from "node:crypto";
import { getPrivateKey, env } from "../config";
import type { AccountRole } from "../dto/auth.dto";

const ALG = "RS256";
const ISSUER = "skyhub-user-service";

// Seconds. Single source of truth for both the JWT `exp` and the response `expiresIn`.
export const ACCESS_TOKEN_TTL_SECONDS = env.JWT_ACCESS_TOKEN_TTL;

/**
 * Build and sign a short-lived RS256 access token for a logged-in user.
 * - sub  = user id (who the token is about)
 * - role = our custom claim (for authorization without a DB lookup)
 * - jti  = unique id (so we can revoke this exact token later, AUTH 4)
 * - kid  = which key signed it (in the header, for rotation later)
 * Signed with the in-memory private key loaded at startup (Step 2).
 */
export async function signAccessToken(payload: {
  sub: string;
  role: AccountRole;
}): Promise<string> {
  return await new SignJWT({ role: payload.role })
    .setProtectedHeader({ alg: ALG, kid: env.JWT_KEY_ID, typ: "JWT" })
    .setSubject(payload.sub)
    .setJti(randomUUID())
    .setIssuer(ISSUER)
    .setIssuedAt()
    .setExpirationTime(`${ACCESS_TOKEN_TTL_SECONDS}s`)
    .sign(getPrivateKey());
}


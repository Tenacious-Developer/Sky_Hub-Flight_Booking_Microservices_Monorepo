import { readFileSync } from "node:fs";
import { importPKCS8, importSPKI } from "jose";
import { env } from "./env";

// RS256 = RSA signature with SHA-256. Must match what we put in the JWT header later.
const ALG = "RS256";

// Parsed keys, held in memory after loadKeys() runs at startup.
// jose v6 returns the Web Crypto CryptoKey type (it dropped the old `KeyLike` alias).
let privateKey: CryptoKey;
let publicKey: CryptoKey;

/**
 * Read both PEM files from disk and parse them into key objects jose can use.
 * Called ONCE at server startup — not on every login (file I/O + parsing is slow).
 * If a key file is missing/corrupt, this throws and the server refuses to start.
 */
export async function loadKeys(): Promise<void> {
  const privatePem = readFileSync(env.JWT_PRIVATE_KEY_PATH, "utf8");
  const publicPem = readFileSync(env.JWT_PUBLIC_KEY_PATH, "utf8");

  // importPKCS8 = parse a PRIVATE key (PKCS#8 format, what openssl genpkey produced).
  privateKey = await importPKCS8(privatePem, ALG);
  // importSPKI  = parse a PUBLIC key (SPKI format, what openssl -pubout produced).
  publicKey = await importSPKI(publicPem, ALG);
}

/** The private key — used by token.service to SIGN access tokens (Step 3). */
export function getPrivateKey(): CryptoKey {
  if (!privateKey) throw new Error("Keys not loaded — call loadKeys() at startup");
  return privateKey;
}

/** The public key — used to VERIFY tokens later (AUTH 2 / requireAuth). */
export function getPublicKey(): CryptoKey {
  if (!publicKey) throw new Error("Keys not loaded — call loadKeys() at startup");
  return publicKey;
}

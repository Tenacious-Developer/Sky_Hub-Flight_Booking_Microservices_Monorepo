import "dotenv/config";
import { z } from "zod";
import { logger } from "@skyhub/common-utils";

/**
 * Single source of truth for this service's environment.
 * Parsed once at startup — the process crashes with a clear message if any
 * variable is missing/malformed, instead of silently defaulting at call sites.
 */
const envSchema = z.object({
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
  PORT: z.coerce.number().int().positive().default(3001),
  DATABASE_URL: z.string().min(1, "DATABASE_URL is required"),
  LOG_LEVEL: z
    .enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"])
    .default("info"),
  // "*" (default) or a comma-separated list of allowed origins.
  CORS_ORIGIN: z.string().default("*"),
  // bcrypt cost factor (§3.2). 12 ≈ 250ms/hash — slow enough to resist brute force.
  BCRYPT_ROUNDS: z.coerce.number().int().min(10).max(15).default(12),
  // JWT signing (RS256). Keys live in files, gitignored — never in source/env directly.
  JWT_PRIVATE_KEY_PATH: z.string().min(1, "JWT_PRIVATE_KEY_PATH is required"),
  JWT_PUBLIC_KEY_PATH: z.string().min(1, "JWT_PUBLIC_KEY_PATH is required"),
  JWT_KEY_ID: z.string().min(1, "JWT_KEY_ID is required"),
  // Access-token lifetime in SECONDS. One source of truth for the JWT `exp`
  // claim AND the `expiresIn` we return to the client. 900 = 15 min.
  JWT_ACCESS_TOKEN_TTL: z.coerce.number().int().positive().default(900),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues
    .map((i) => `  - ${i.path.join(".") || "(root)"}: ${i.message}`)
    .join("\n");
  logger.fatal(`Invalid environment configuration:\n${issues}`);
  process.exit(1);
}

export const env = parsed.data;
export type Env = typeof env;
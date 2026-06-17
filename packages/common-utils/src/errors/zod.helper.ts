import { ZodError } from "zod";
import type { ErrorDetail } from "@skyhub/shared-types";

/**
 * Maps a ZodError's issues into our ErrorDetail[] shape (field + message).
 * Single source of truth — used by both the validate middleware and the error handler.
 */
export const zodToDetails = (err: ZodError): ErrorDetail[] =>
  err.issues.map((i) => ({
    field: i.path.join(".") || "root",
    message: i.message,
  }));

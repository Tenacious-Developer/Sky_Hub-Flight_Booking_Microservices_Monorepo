import { Prisma } from "./generated/prisma/client";
import {
  ConflictError, NotFoundError, BadRequestError,
  ServiceUnavailableError, InternalServerError, AppError,
} from "@skyhub/common-utils";

export function mapPrismaError(err: unknown): AppError {
  // Already a domain error (e.g. a guard-throw inside the repo) — pass it through
  // unchanged instead of masking it as a generic 500.
  if (err instanceof AppError) return err;

  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    switch (err.code) {
      case "P2002": return new ConflictError("Resource already exists", undefined, err);
      case "P2025": return new NotFoundError("Resource not found", err);
      case "P2003": return new ConflictError("Related resource constraint failed", undefined, err);
      case "P2011": return new BadRequestError("Missing required field", undefined, err);
      case "P2000": return new BadRequestError("Value too long for field", undefined, err);
      case "P2034": return new ConflictError("Write conflict, please retry", undefined, err);
      default:      return new BadRequestError("Database request error", undefined, err);
    }
  }
  if (err instanceof Prisma.PrismaClientValidationError) return new BadRequestError("Invalid query", undefined, err);
  if (err instanceof Prisma.PrismaClientInitializationError) return new ServiceUnavailableError("Database unavailable", err);
  return new InternalServerError("Internal server error", err); // unknown → masked 500, root cause preserved in `cause`
}
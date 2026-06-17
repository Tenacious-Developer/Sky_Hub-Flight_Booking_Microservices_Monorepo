import { StatusCodes } from "http-status-codes";
import type { ErrorDetail } from "@skyhub/shared-types";

export class AppError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number,
    public readonly code: string,
    public readonly details?: ErrorDetail[],
    public readonly isOperational = true,
    options?: ErrorOptions, // carries `cause` so the root error is never lost
  ) {
    super(message, options);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

// ---- 4xx: client's fault (operational) ----
export class BadRequestError extends AppError {
  constructor(message = "Bad request", details?: ErrorDetail[], cause?: unknown) {
    super(message, StatusCodes.BAD_REQUEST, "BAD_REQUEST", details, true, { cause });
  }
}
export class ValidationError extends AppError {
  constructor(message = "Validation failed", details?: ErrorDetail[], cause?: unknown) {
    super(message, StatusCodes.UNPROCESSABLE_ENTITY, "VALIDATION_ERROR", details, true, { cause });
  }
}
export class UnauthorizedError extends AppError {
  constructor(message = "Authentication required", cause?: unknown) {
    super(message, StatusCodes.UNAUTHORIZED, "UNAUTHORIZED", undefined, true, { cause });
  }
}
export class ForbiddenError extends AppError {
  constructor(message = "Permission denied", cause?: unknown) {
    super(message, StatusCodes.FORBIDDEN, "FORBIDDEN", undefined, true, { cause });
  }
}
export class NotFoundError extends AppError {
  constructor(message = "Resource not found", cause?: unknown) {
    super(message, StatusCodes.NOT_FOUND, "NOT_FOUND", undefined, true, { cause });
  }
}
export class ConflictError extends AppError {
  constructor(message = "Resource already exists", details?: ErrorDetail[], cause?: unknown) {
    super(message, StatusCodes.CONFLICT, "CONFLICT", details, true, { cause });
  }
}
export class TooManyRequestsError extends AppError {
  constructor(message = "Too many requests", cause?: unknown) {
    super(message, StatusCodes.TOO_MANY_REQUESTS, "RATE_LIMITED", undefined, true, { cause });
  }
}

// ---- 5xx: server's fault ----
export class InternalServerError extends AppError {
  // isOperational = false → at process level, signals a real bug
  constructor(message = "Internal server error", cause?: unknown) {
    super(message, StatusCodes.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", undefined, false, { cause });
  }
}
export class ServiceUnavailableError extends AppError {
  constructor(message = "Service temporarily unavailable", cause?: unknown) {
    super(message, StatusCodes.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", undefined, true, { cause });
  }
}

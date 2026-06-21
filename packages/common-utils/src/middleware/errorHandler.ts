import { Request, Response, NextFunction } from "express";
import { ZodError } from "zod";
import { StatusCodes } from "http-status-codes";
import type { ErrorDetail, ErrorResponse } from "@skyhub/shared-types";
import { AppError } from "../errors/app.error.js";
import { zodToDetails } from "../errors/zod.helper.js";
import { logger } from "../logger/logger.js";
import { getCorrelationId } from "../context/request-context.js"; 

const isProd = process.env.NODE_ENV === "production";        

function respond(res: Response, status: number, name: string, message: string, details: ErrorDetail[] = []) {
  const body: ErrorResponse = {
    success: false,
    error: { statusCode: status, name, message, details },
    traceId: getCorrelationId() ?? "unknown", // ← from context, mirrors x-correlation-id header
  };
  res.status(status).json(body);
}

export function globalErrorHandler(err: unknown, _req: Request, res: Response, _next: NextFunction): void {
  // Our own errors — trusted
  if (err instanceof AppError) {
    // log level by fault: 5xx = server's problem (alert), 4xx = client's problem (low priority)
    if (err.statusCode >= 500) logger.error({ err }, err.message);
    else logger.warn({ data: { code: err.code } }, err.message);
    return respond(res, err.statusCode, err.code, err.message, err.details);
  }

  // Zod — validation
  if (err instanceof ZodError) {
    const details = zodToDetails(err);
    logger.warn({ data: { details } }, "validation failed");
    return respond(res, StatusCodes.UNPROCESSABLE_ENTITY, "VALIDATION_ERROR", "Validation failed", details);
  }
  // Malformed JSON
  if (err instanceof SyntaxError && "body" in (err as object)) {
    logger.warn("malformed JSON in request body");
    return respond(res, StatusCodes.BAD_REQUEST, "MALFORMED_JSON", "Malformed JSON in request body");
  }

  // Unknown / programmer error → LOG fully, MASK from client
  logger.error({ err }, "unhandled error");
  respond(res, StatusCodes.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", isProd ? "An unexpected error occurred" : String((err as { message?: string })?.message ?? err));
}

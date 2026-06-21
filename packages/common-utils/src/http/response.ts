import { Response } from "express";
import { StatusCodes } from "http-status-codes";
import type { SuccessResponse, PaginationMeta } from "@skyhub/shared-types";
import { getCorrelationId } from "../context/request-context.js";

/** 200 OK with data (optionally paginated). */
export function ok<T>(res: Response, data: T, meta?: PaginationMeta, message?: string): void {
  const traceId = getCorrelationId();
  const body: SuccessResponse<T> = {
    success: true,
    ...(message ? { message } : {}),
    data,
    ...(meta ? { meta } : {}),
    ...(traceId ? { traceId } : {}),
  };
  res.status(StatusCodes.OK).json(body);
}

/** 201 Created with the new resource. */
export function created<T>(res: Response, data: T, message?: string): void {
  const traceId = getCorrelationId();
  const body: SuccessResponse<T> = {
    success: true,
    ...(message ? { message } : {}),
    data,
    ...(traceId ? { traceId } : {}),
  };
  res.status(StatusCodes.CREATED).json(body);
}

/** 200 OK with only a confirmation message and no resource body (e.g. after a delete). */
export function okMessage(res: Response, message: string): void {
  const traceId = getCorrelationId();
  const body = {
    success: true as const,
    message,
    ...(traceId ? { traceId } : {}),
  };
  res.status(StatusCodes.OK).json(body);
}

/** 204 No Content (e.g. after a delete) — no body. */
export function noContent(res: Response): void {
  res.status(StatusCodes.NO_CONTENT).send();
}


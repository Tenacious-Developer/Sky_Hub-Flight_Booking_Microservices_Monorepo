import { Response } from "express";
import { StatusCodes } from "http-status-codes";
import type { SuccessResponse, PaginationMeta } from "@skyhub/shared-types";

/** 200 OK with data (optionally paginated). */
export function ok<T>(res: Response, data: T, meta?: PaginationMeta): void {
  const body: SuccessResponse<T> = { success: true, data, ...(meta ? { meta } : {}) };
  res.status(StatusCodes.OK).json(body);
}

/** 201 Created with the new resource. */
export function created<T>(res: Response, data: T): void {
  const body: SuccessResponse<T> = { success: true, data };
  res.status(StatusCodes.CREATED).json(body);
}

/** 204 No Content (e.g. after a delete) — no body. */
export function noContent(res: Response): void {
  res.status(StatusCodes.NO_CONTENT).send();
}

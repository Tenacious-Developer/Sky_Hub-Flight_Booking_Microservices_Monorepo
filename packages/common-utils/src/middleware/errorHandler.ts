// import { NextFunction, Request, Response } from 'express';
// import { ErrorCode } from '@skyhub/shared-types';
// import { AppError, ValidationDetail } from '../errors/AppError.js';

// const isDev = process.env.NODE_ENV === 'development';

// function isZodError(
//   err: unknown,
// ): err is { name: string; issues: Array<{ path: (string | number)[]; message: string }> } {
//   return (
//     typeof err === 'object' &&
//     err !== null &&
//     (err as { name?: unknown }).name === 'ZodError' &&
//     Array.isArray((err as { issues?: unknown }).issues)
//   );
// }

// function getTraceId(req: Request): string {
//   const header = req.headers['x-correlation-id'];
//   if (typeof header === 'string') return header;
//   return `tr-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
// }

// function buildErrorBody(
//   code: ErrorCode,
//   message: string,
//   details: ValidationDetail[] | undefined,
//   traceId: string,
// ) {
//   return {
//     success: false,
//     error: {
//       code,
//       message,
//       ...(details && details.length > 0 && { details }),
//     },
//     traceId,
//   };
// }

// export function globalErrorHandler(
//   err: unknown,
//   req: Request,
//   res: Response,
//   _next: NextFunction,
// ): void {
//   const traceId = getTraceId(req);

//   if (isZodError(err)) {
//     const details: ValidationDetail[] = err.issues.map((issue) => ({
//       field: issue.path.join('.') || 'root',
//       message: issue.message,
//     }));
//     res
//       .status(400)
//       .json(buildErrorBody(ErrorCode.VALIDATION_ERROR, 'Validation failed', details, traceId));
//     return;
//   }

//   if (err instanceof AppError && err.isOperational) {
//     console.warn(`[${traceId}] ${err.code}: ${err.message}`);
//     res.status(err.statusCode).json(buildErrorBody(err.code, err.message, err.details, traceId));
//     return;
//   }

//   console.error(`[${traceId}] Unhandled error:`, err);
//   res.status(500).json(
//     buildErrorBody(
//       ErrorCode.INTERNAL_ERROR,
//       isDev && err instanceof Error ? err.message : 'An unexpected error occurred',
//       undefined,
//       traceId,
//     ),
//   );
// }


import { NextFunction, Request, Response } from 'express';
import { AppError } from '../errors/AppError.js';

function getTraceId(req: Request): string {
  const header = req.headers['x-correlation-id'];
  if (typeof header === 'string') return header;
  return `tr-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
}

/**
 * Global 4-argument Express error handling middleware.
 */
export function globalErrorHandler(
  err: unknown,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  const traceId = getTraceId(req);

  // 1. If it is our standardized AppError
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      success: false,
      error: {
        statusCode: err.statusCode,
        name: err.name,
        message: err.message,
        ...(err.details && err.details.length > 0 && { details: err.details })
      },
      traceId
    });
    return;
  }

  // 2. Parse and handle Zod schema validation errors natively
  if (
    typeof err === 'object' &&
    err !== null &&
    (err as { name?: unknown }).name === 'ZodError' &&
    Array.isArray((err as { issues?: unknown }).issues)
  ) {
    const zodIssues = (err as { issues: Array<{ path: (string | number)[]; message: string }> }).issues;
    const details = zodIssues.map((issue) => ({
      field: issue.path.join('.') || 'root',
      message: issue.message,
    }));

    res.status(400).json({
      success: false,
      error: {
        statusCode: 400,
        name: 'VALIDATION_ERROR',
        message: 'Validation failed',
        details
      },
      traceId
    });
    return;
  }

  // 3. Mask unexpected system crashes (e.g. database connectivity loss)
  console.error(`[${traceId}] Unhandled server crash:`, err);
  
  const isDev = process.env.NODE_ENV === 'development';
  res.status(500).json({
    success: false,
    error: {
      statusCode: 500,
      name: 'INTERNAL_SERVER_ERROR',
      message: isDev && err instanceof Error ? err.message : 'An unexpected error occurred'
    },
    traceId
  });
}



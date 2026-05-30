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

export function globalErrorHandler(
  err: unknown,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  // 1. If it is our simple custom AppError, send the custom status and message
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      success: false,
      message: err.message
    });
    return;
  }

  // 2. If it is a completely unexpected crash (like reading undefined or DB timeout)
  console.error('Unhandled server error caught in middleware:', err);
  res.status(500).json({
    success: false,
    message: err instanceof Error ? err.message : 'An unexpected error occurred'
  });
}


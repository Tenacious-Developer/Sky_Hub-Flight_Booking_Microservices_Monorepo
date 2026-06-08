// import { Request, Response } from 'express';
// import { AppError } from '../errors/AppError.js';

// export function notFoundHandler(req: Request, res: Response): void {
//   const err = AppError.notFound(`Cannot ${req.method} ${req.path}`);
//   const traceId = req.headers['x-correlation-id'];
//   res.status(err.statusCode).json({
//     success: false,
//     error: {
//       code: err.code,
//       message: err.message,
//     },
//     traceId: typeof traceId === 'string' ? traceId : 'unknown',
//   });
// }


import { Request, Response } from 'express';
import { AppError } from '../errors/AppError.js';

/**
 * Catches all unmatched paths and translates them into a clean 404 AppError
 */
export function notFoundHandler(req: Request, res: Response): void {
  throw new AppError(`Cannot ${req.method} ${req.path}`, 404, 'NOT_FOUND');
}

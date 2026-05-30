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



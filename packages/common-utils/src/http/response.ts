// import { Response } from 'express';

// export interface PaginationMeta {
//   page: number;
//   limit: number;
//   total: number;
// }

// function resolveTraceId(res: Response): string {
//   const header = res.req?.headers?.['x-correlation-id'];
//   return typeof header === 'string' ? header : 'unknown';
// }

// export function ok<T>(res: Response, data: T, message = 'Success', meta?: PaginationMeta): void {
//   res.status(200).json({
//     success: true,
//     message,
//     data,
//     ...(meta && { meta }),
//     traceId: resolveTraceId(res),
//   });
// }

// export function created<T>(res: Response, data: T, message = 'Created'): void {
//   res.status(201).json({
//     success: true,
//     message,
//     data,
//     traceId: resolveTraceId(res),
//   });
// }

// export function noContent(res: Response): void {
//   res.status(204).send();
// }

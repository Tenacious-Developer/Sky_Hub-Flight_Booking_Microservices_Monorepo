import { Request, Response, NextFunction } from "express";
import { logger } from "../logger/logger.js";

/**
 * Logs one line per request AFTER the response finishes, capturing the final
 * status code and duration. correlationId is auto-attached by the logger's mixin.
 * Register right after the correlationId middleware (so it runs inside the context).
 */
export function requestLogger(req: Request, res: Response, next: NextFunction): void {
  // Skip health/readiness probes — they're high-frequency and would drown the logs.
  if (req.originalUrl.includes("/health")) return next();

  const start = Date.now();

  res.on("finish", () => {
    const durationMs = Date.now() - start;
    logger.info(
      {
        data: {
          method: req.method,
          path: req.originalUrl,
          status: res.statusCode,
          durationMs,
        },
      },
      "request completed",
    );
  });

  next();
}

import type { Server } from "http";
import { logger } from "../logger/logger.js";

/**
 * Registers process-level safety nets:
 * - unhandledRejection / uncaughtException → log + graceful shutdown (exit 1)
 * - SIGTERM / SIGINT → graceful shutdown (exit 0)
 *
 * `onShutdown` lets the service release resources (prisma, pools, etc.).
 * Each shutdown path force-exits after 10s if `server.close` hangs.
 */
export function registerProcessHandlers(
  server?: Server,
  onShutdown?: () => Promise<void>,
): void {
  let shuttingDown = false;

  const shutdown = (code: number) => {
    if (shuttingDown) return;
    shuttingDown = true;

    const finalize = async () => {
      try {
        await onShutdown?.();
      } catch (err) {
        logger.error({ err }, "Shutdown cleanup failed");
      }
      process.exit(code);
    };

    if (server) {
      server.close(() => void finalize());
      setTimeout(() => process.exit(code), 10_000).unref(); // force-exit if close hangs
    } else {
      void finalize();
    }
  };

  process.on("unhandledRejection", (reason) => {
    logger.error({ err: reason }, "Unhandled promise rejection — shutting down");
    shutdown(1);
  });

  process.on("uncaughtException", (err) => {
    logger.error({ err }, "Uncaught exception — shutting down");
    shutdown(1);
  });

  for (const sig of ["SIGTERM", "SIGINT"] as const) {
    process.on(sig, () => {
      logger.info({ data: { signal: sig } }, "Signal received — graceful shutdown");
      shutdown(0);
    });
  }
}

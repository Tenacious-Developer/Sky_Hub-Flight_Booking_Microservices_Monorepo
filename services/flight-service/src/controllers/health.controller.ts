import { Request, Response, NextFunction } from "express";
import { ok, ServiceUnavailableError } from "@skyhub/common-utils";
import { prisma } from "../config/index";

export const healthHandler = async (_req: Request, res: Response, next: NextFunction) => {
  try {
    let database: "ok" | "error" = "ok";
    try {
      await prisma.$queryRaw`SELECT 1`;
    } catch {
      database = "error";
    }

    if (database !== "ok") {
      // routed through the global error handler → standard 503 error envelope
      throw new ServiceUnavailableError("Database unavailable");
    }

    ok(res, {
      status: "healthy",
      service: "flight-service",
      timestamp: new Date().toISOString(),
      checks: { database },
    });
  } catch (err) {
    next(err);
  }
};

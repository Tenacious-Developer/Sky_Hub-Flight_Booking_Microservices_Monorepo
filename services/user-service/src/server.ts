import express from "express";
import helmet from "helmet";
import cors from "cors";
import {
  correlationId,
  requestLogger,
  globalErrorHandler,
  notFoundHandler,
  registerProcessHandlers,
  logger,
} from "@skyhub/common-utils";
import { Config, prisma, loadKeys } from './config/index';
import router from "./routers/index.router";

async function startUserServer() {
  const app = express();

  const corsOrigin =
    Config.env.CORS_ORIGIN === "*"
      ? "*"
      : Config.env.CORS_ORIGIN.split(",").map((o) => o.trim());

  app.set("trust proxy", 1);          // behind the API gateway — trust X-Forwarded-* for client IP
  app.use(helmet());                  // secure HTTP headers
  app.use(cors({ origin: corsOrigin }));
  app.use(correlationId);
  app.use(requestLogger);
  app.use(express.json({ limit: "100kb" }));
  app.use('/api', router);

  // Register global error handlers AFTER routes (so they catch everything)
  app.use(notFoundHandler);
  app.use(globalErrorHandler);

  try {
    await loadKeys();
    logger.info("JWT signing keys loaded");
    await prisma.$connect();
    logger.info("Database connected successfully");
  } catch (err) {
    logger.fatal({ err }, "Startup failed");
    process.exit(1);
  }

  const server = app.listen(Config.server.port, () => {
    logger.info({ data: { port: Config.server.port } }, `User Service is running on port ${Config.server.port}`);
  });

  registerProcessHandlers(server, async () => {
    await prisma.$disconnect();
    logger.info("Database disconnected");
  });
}

startUserServer();
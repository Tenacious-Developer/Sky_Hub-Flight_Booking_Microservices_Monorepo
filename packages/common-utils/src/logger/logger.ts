import pino from "pino";
import { getCorrelationId } from "../context/request-context.js";

export const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  formatters: {
    level: (label: string) => ({ level: label }),
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  // serialize Error objects (stack + `cause` chain) when logged under `err`
  serializers: { err: pino.stdSerializers.err },
  mixin() {
    const correlationId = getCorrelationId();
    return correlationId ? { correlationId } : {};
  },
});



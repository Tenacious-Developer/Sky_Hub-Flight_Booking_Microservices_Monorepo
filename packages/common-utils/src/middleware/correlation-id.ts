import { randomUUID } from "node:crypto";
import { Request, Response, NextFunction } from "express";
import { runWithContext } from "../context/request-context.js";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function correlationId(req: Request, res: Response, next: NextFunction): void {
  const incoming = req.headers["x-correlation-id"];
  // only trust a well-formed UUID from upstream — otherwise mint our own.
  // prevents log injection / unbounded values from a spoofed header.
  const id = typeof incoming === "string" && UUID_RE.test(incoming) ? incoming : randomUUID();
  res.setHeader("x-correlation-id", id);          // echo back to client
  runWithContext({ correlationId: id }, () => next()); // everything after runs in context
}

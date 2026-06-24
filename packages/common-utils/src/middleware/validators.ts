import { Request, Response, NextFunction } from "express";
import { ZodSchema, ZodError } from "zod";
import { ValidationError } from "../errors/app.error.js";
import { zodToDetails } from "../errors/zod.helper.js";

type RequestSchemas = {
  body?: ZodSchema;
  params?: ZodSchema;
  query?: ZodSchema;
};

/**
 * Validates any combination of request body / params / query against Zod schemas.
 *
 * - `body` is reassigned with the parsed result, so Zod transforms / coercions
 *   persist for the controller (e.g. `code` uppercased). `req.body` is typed `any`.
 * - `params` & `query` are validated only (not reassigned): `req.params` is a typed
 *   `ParamsDictionary` and Express 5 exposes `req.query` as a read-only getter, so
 *   reassigning either is unsafe. Validation still rejects bad input.
 *
 * On failure → ValidationError(422) forwarded to the global error handler.
 */
export const validateRequest = (schemas: RequestSchemas) => {
  return async (req: Request, _res: Response, next: NextFunction) => {
    try {
      if (schemas.body) req.body = await schemas.body.parseAsync(req.body);
      if (schemas.params) await schemas.params.parseAsync(req.params); // validate only
      if (schemas.query) {
        // Express 5 makes req.query a read-only getter, so a plain reassign throws.
        // defineProperty lets the parsed value (with Zod coercions/transforms, e.g.
        // string "150" → number 150) persist for controllers — otherwise the
        // original string query reaches the repo and Prisma rejects it.
        const parsedQuery = await schemas.query.parseAsync(req.query);
        Object.defineProperty(req, "query", { value: parsedQuery, configurable: true });
      }
      next();
    } catch (err) {
      if (err instanceof ZodError) {
        return next(new ValidationError("Validation failed", zodToDetails(err)));
      }
      next(err); // non-Zod → bubble to the global handler
    }
  };
};


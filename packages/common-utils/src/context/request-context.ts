import { AsyncLocalStorage } from "node:async_hooks";

type RequestContext = { correlationId: string };

const storage = new AsyncLocalStorage<RequestContext>();

// run the rest of the request "inside" this context
export function runWithContext<T>(ctx: RequestContext, fn: () => T): T {
  return storage.run(ctx, fn);
}

// any code anywhere in the request can read this
export function getCorrelationId(): string | undefined {
  return storage.getStore()?.correlationId;
}

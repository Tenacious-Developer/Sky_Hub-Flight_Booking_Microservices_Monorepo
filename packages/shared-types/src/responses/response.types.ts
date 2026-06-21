export type PaginationMeta = {
  page: number;
  limit: number;
  total: number;
};

export type SuccessResponse<T> = {
  success: true;
  message?: string; // human-readable summary, e.g. "Airport created successfully."
  data: T;
  meta?: PaginationMeta; // present only for paginated list responses
  traceId?: string; // correlation id for this request (mirrors x-correlation-id header)
};

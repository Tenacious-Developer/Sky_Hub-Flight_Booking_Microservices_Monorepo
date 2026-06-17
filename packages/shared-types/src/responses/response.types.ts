export type PaginationMeta = {
  page: number;
  limit: number;
  total: number;
};

export type SuccessResponse<T> = {
  success: true;
  data: T;
  meta?: PaginationMeta; // present only for paginated list responses
};

export type ErrorDetail = {
  field: string;
  message: string;
};

export type ErrorResponse = {
  success: false;
  error: {
    statusCode: number;
    name: string;
    message: string;
    details: ErrorDetail[];
  };
  traceId: string; // correlation id for this request (mirrors x-correlation-id header)
};

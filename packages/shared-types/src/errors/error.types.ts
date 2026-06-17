export type ErrorDetail = {
  field: string;
  message: string;
};

export type ErrorResponse = {
  success: false;
  error: {
    code: string;
    message: string;
    details?: ErrorDetail[];
  };
  meta: {
    correlationId: string;
    timestamp: string;
  };
};

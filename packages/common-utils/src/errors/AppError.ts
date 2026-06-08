// import { ErrorCode } from '@skyhub/shared-types';
// import { HTTP_STATUS } from './httpStatus.js';

// export interface ValidationDetail {
//   field: string;
//   message: string;
// }

// export class AppError extends Error {
//   readonly code: ErrorCode;
//   readonly statusCode: number;
//   readonly details?: ValidationDetail[];
//   readonly isOperational: boolean;

//   private constructor(
//     code: ErrorCode,
//     message: string,
//     details?: ValidationDetail[],
//     isOperational = true,
//   ) {
//     super(message);
//     this.name = 'AppError';
//     this.code = code;
//     this.statusCode = HTTP_STATUS[code];
//     this.details = details;
//     this.isOperational = isOperational;
//     Error.captureStackTrace(this, this.constructor);
//   }

//   static notFound(message = 'Resource not found'): AppError {
//     return new AppError(ErrorCode.NOT_FOUND, message);
//   }

//   static unauthorized(message = 'Authentication required'): AppError {
//     return new AppError(ErrorCode.UNAUTHORIZED, message);
//   }

//   static tokenExpired(): AppError {
//     return new AppError(ErrorCode.TOKEN_EXPIRED, 'Token has expired');
//   }

//   static tokenBlacklisted(): AppError {
//     return new AppError(ErrorCode.TOKEN_BLACKLISTED, 'Token has been revoked');
//   }

//   static forbidden(message = 'Insufficient permissions'): AppError {
//     return new AppError(ErrorCode.FORBIDDEN, message);
//   }

//   static conflict(message: string): AppError {
//     return new AppError(ErrorCode.CONFLICT, message);
//   }

//   static businessRule(message: string): AppError {
//     return new AppError(ErrorCode.BUSINESS_RULE_VIOLATION, message);
//   }

//   static rateLimitExceeded(): AppError {
//     return new AppError(ErrorCode.RATE_LIMIT_EXCEEDED, 'Too many requests');
//   }

//   static serviceUnavailable(message = 'Service temporarily unavailable'): AppError {
//     return new AppError(ErrorCode.SERVICE_UNAVAILABLE, message);
//   }

//   static validation(details: ValidationDetail[], message = 'Validation failed'): AppError {
//     return new AppError(ErrorCode.VALIDATION_ERROR, message, details);
//   }
// }



const ERROR_NAME_MAP: Record<number, string> = {
  400: 'BAD_REQUEST',
  401: 'UNAUTHORIZED',
  403: 'FORBIDDEN',
  404: 'NOT_FOUND',
  409: 'CONFLICT',
  422: 'UNPROCESSABLE_ENTITY',
  423: 'LOCKED',
  429: 'TOO_MANY_REQUESTS',
  500: 'INTERNAL_SERVER_ERROR',
  503: 'SERVICE_UNAVAILABLE',
};

function getErrorNameByStatus(status: number): string {
  return ERROR_NAME_MAP[status] || 'INTERNAL_SERVER_ERROR';
}

/**
 * The Standardized Application Error Class
 * Extends the native JavaScript Error to include numeric statusCode and string name.
 */
export class AppError extends Error {
  public readonly statusCode: number;
  public readonly name: string;
  public readonly details?: any[];
  public readonly isOperational: boolean = true;

  constructor(message: string, statusCode: number, name?: string, details?: any[]) {
    // 1. Pass the human-readable message to the built-in Error constructor
    super(message);
    
    // 2. Assign the numeric HTTP status code
    this.statusCode = statusCode;
    
    // 3. Assign the machine-readable error name (defaults to standard mapping if not specified)
    this.name = name || getErrorNameByStatus(statusCode);
    
    // 4. Assign optional error/validation fields array
    this.details = details;

    // 5. Preserve the native V8 JavaScript engine stack trace
    Error.captureStackTrace(this, this.constructor);
  }
}


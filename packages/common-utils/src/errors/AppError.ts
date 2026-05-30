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



/**
 * THE CORE BASE APPLICATION ERROR
 * Inherits natively from the JavaScript built-in Error object.
 */
export class AppError extends Error {
  public readonly statusCode: number;
  public readonly isOperational: boolean = true;

  constructor(message: string, statusCode: number) {
    // 1. Pass the message to the built-in JavaScript Error constructor
    super(message);

    // 2. Attach your custom HTTP status code property
    this.statusCode = statusCode;

    // 3. SET THE NAME: Override the generic "Error" name with the exact class constructor name
    // This makes sure your logs print "AppError" instead of just "Error"
    this.name = this.constructor.name;

    // 4. Preserve the native V8 JavaScript stack trace
    Error.captureStackTrace(this, this.constructor);
  }
}

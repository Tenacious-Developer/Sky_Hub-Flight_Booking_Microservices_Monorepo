import { Router } from "express";
import { validateRequest } from "@skyhub/common-utils";
import { registerHandler, verifyEmailHandler, resendVerificationHandler } from "../../controllers/auth.controller";
import { registerSchema, verifyEmailSchema, resendVerificationSchema } from "../../validators/auth.validator";

const authRouter = Router();

authRouter.post('/register', validateRequest({ body: registerSchema }), registerHandler);
authRouter.post('/verify-email', validateRequest({ body: verifyEmailSchema }), verifyEmailHandler);
authRouter.post('/resend-verification', validateRequest({ body: resendVerificationSchema }), resendVerificationHandler);

export default authRouter;
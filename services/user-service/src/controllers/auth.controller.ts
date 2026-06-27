import { Request, Response } from "express";
import { ok, created, okMessage } from "@skyhub/common-utils";
import { registerService, verifyEmailService, resendVerificationService, loginService } from "../services/auth.service";
import { RegisterDTO, VerifyEmailDTO, ResendVerificationDTO, LoginDTO } from "../dto/auth.dto";

export const registerHandler = async (
    req: Request<unknown, unknown, RegisterDTO>,
    res: Response,
): Promise<void> => {
    const user = await registerService(req.body);
    created(res, user, "Registration successful. Please verify your email.");
};

export const verifyEmailHandler = async (
    req: Request<unknown, unknown, VerifyEmailDTO>,
    res: Response,
): Promise<void> => {
    await verifyEmailService(req.body);
    okMessage(res, "Email verified successfully. You can now log in.");
};

export const resendVerificationHandler = async (
    req: Request<unknown, unknown, ResendVerificationDTO>,
    res: Response,
): Promise<void> => {
    await resendVerificationService(req.body);
    okMessage(res, "If this account is unverified, a new 6-digit code has been sent.");
};

export const loginHandler = async (
    req: Request<unknown, unknown, LoginDTO>,
    res: Response,
): Promise<void> => {
    const result = await loginService(req.body);
    ok(res, result, undefined, "Login successful");
};
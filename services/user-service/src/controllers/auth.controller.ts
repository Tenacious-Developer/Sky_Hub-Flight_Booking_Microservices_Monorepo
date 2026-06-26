import { Request, Response } from "express";
import { created, okMessage } from "@skyhub/common-utils";
import { registerService, verifyEmailService, resendVerificationService } from "../services/auth.service";
import { RegisterDTO, VerifyEmailDTO, ResendVerificationDTO } from "../dto/auth.dto";

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
import bcrypt from "bcrypt";
import {
    logger,
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    TooManyRequestsError,
} from "@skyhub/common-utils";
import { env } from "../config";
import {
    createUser,
    findUserForVerification,
    markEmailVerified,
    bumpVerifyAttempts,
    clearVerifyToken,
    setEmailVerifyToken,
    findUserForLogin,
    recordFailedLogin,
    recordSuccessfulLogin,
} from "../repositories/user.repository";
import { generateOtp, hashOtp } from "../utils/crypto.utils";
import { sendVerificationCode } from "./notification.service";
import { signAccessToken, ACCESS_TOKEN_TTL_SECONDS } from "./token.service";
import {
    RegisterDTO,
    RegisterResponseDTO,
    toRegisterResponse,
    VerifyEmailDTO,
    ResendVerificationDTO,
    LoginDTO,
    LoginResponseDTO,
} from "../dto/auth.dto";

const OTP_EXPIRY_MS = 5 * 60 * 1000; // 5 minutes — the primary OTP defense
const MAX_VERIFY_ATTEMPTS = 5;
// One generic message for EVERY failure mode (anti-enumeration): an attacker can't
// tell "no such email" from "wrong code" from "expired" from "already verified".
const INVALID_CODE_MSG = "The verification code is incorrect or has expired. Please request a new code.";

// Same generic message for "no such email" and "wrong password" (anti-enumeration).
const INVALID_CREDENTIALS_MSG = "Invalid email or password.";
// One real bcrypt hash, computed once at startup. Compared against when no user is
// found so the response takes the SAME time as a real compare (no timing leak).
const DUMMY_HASH = bcrypt.hashSync("dummy-password-for-timing-safety", env.BCRYPT_ROUNDS);

export async function registerService(input: RegisterDTO): Promise<RegisterResponseDTO> {
    // Hash the password — the raw value is never stored. ~250ms by design (cost 12, §3.2).
    const passwordHash = await bcrypt.hash(input.password, env.BCRYPT_ROUNDS);

    // Generate the verification OTP. Only the HASH + expiry are persisted; the raw
    // code goes to the delivery layer (sendVerificationCode) and nowhere else.
    const otp = generateOtp();
    const emailVerifyToken = hashOtp(otp);
    const emailVerifyExpiresAt = new Date(Date.now() + OTP_EXPIRY_MS);

    // No pre-check for an existing email (TOCTOU race) — the DB @unique constraint +
    // the repo's P2002 → 409 translation is the single source of truth for duplicates.
    // User + profile + OTP hash/expiry are written in ONE atomic transaction.
    const user = await createUser({
        email: input.email,
        passwordHash,
        fullName: input.name,
        emailVerifyToken,
        emailVerifyExpiresAt,
    });

    // Persist first, then deliver — never notify about a user that wasn't created.
    await sendVerificationCode(user.email, otp);

    logger.info({ data: { id: user.id, email: user.email } }, "user registered");
    return toRegisterResponse(user);
}

export async function verifyEmailService(input: VerifyEmailDTO): Promise<void> {
    const user = await findUserForVerification(input.email);

    // No user / already verified / no pending code → generic failure (anti-enumeration).
    if (!user || user.emailVerified || !user.emailVerifyToken || !user.emailVerifyExpiresAt) {
        throw new BadRequestError(INVALID_CODE_MSG);
    }

    // Expired code (the primary defense).
    if (user.emailVerifyExpiresAt.getTime() < Date.now()) {
        throw new BadRequestError(INVALID_CODE_MSG);
    }

    // Attempt cap already exhausted.
    if (user.emailVerifyAttempts >= MAX_VERIFY_ATTEMPTS) {
        throw new BadRequestError(INVALID_CODE_MSG);
    }

    // Wrong code → count the attempt; invalidate the code once the cap is hit.
    if (hashOtp(input.code) !== user.emailVerifyToken) {
        const attempts = await bumpVerifyAttempts(user.id);
        if (attempts >= MAX_VERIFY_ATTEMPTS) {
            await clearVerifyToken(user.id); // force a resend
        }
        throw new BadRequestError(INVALID_CODE_MSG);
    }

    // Correct, unexpired, within attempts → verify + single-use clear.
    await markEmailVerified(user.id);
    logger.info({ data: { id: user.id } }, "email verified");
}

export async function resendVerificationService(input: ResendVerificationDTO): Promise<void> {
    const user = await findUserForVerification(input.email);

    // Decoy: silently do nothing for non-existent or already-verified accounts.
    // The controller still returns the SAME generic 200 (anti-enumeration).
    // (Rate-limit deferred to the Redis step — see Rung 2 Step 6 Option C.)
    if (!user || user.emailVerified) {
        return;
    }

    // Issue a fresh code: store the new hash + expiry, reset attempts, then deliver.
    const otp = generateOtp();
    const emailVerifyToken = hashOtp(otp);
    const emailVerifyExpiresAt = new Date(Date.now() + OTP_EXPIRY_MS);

    await setEmailVerifyToken(user.id, emailVerifyToken, emailVerifyExpiresAt);
    await sendVerificationCode(input.email, otp);

    logger.info({ data: { id: user.id } }, "verification code resent");
}

export async function loginService(input: LoginDTO): Promise<LoginResponseDTO> {
    const user = await findUserForLogin(input.email);

    // 1. Lockout — reject before spending a ~250ms bcrypt compare on a locked account.
    if (user?.lockedUntil && user.lockedUntil.getTime() > Date.now()) {
        throw new TooManyRequestsError(
            "Account temporarily locked due to too many failed attempts. Please try again later.",
        );
    }

    // 2. Timing-safe compare — ALWAYS runs (DUMMY_HASH when no user) so response
    //    time can't reveal whether the email is registered.
    const passwordValid = await bcrypt.compare(input.password, user?.passwordHash ?? DUMMY_HASH);

    // 3. Wrong password OR no user → count the failure (only for real users), generic 401.
    if (!user || !passwordValid) {
        if (user) await recordFailedLogin(user.id);
        throw new UnauthorizedError(INVALID_CREDENTIALS_MSG);
    }

    // 4. Password correct → only NOW reveal account-state issues (to the legit owner).
    if (!user.emailVerified) {
        throw new UnauthorizedError("Please verify your email before logging in.");
    }
    if (!user.isActive) {
        throw new ForbiddenError("Your account has been disabled.");
    }

    // 5. Success → clear failures/lock + stamp lastLoginAt.
    await recordSuccessfulLogin(user.id);

    // 6. Issue the access token. expiresIn shares ACCESS_TOKEN_TTL_SECONDS with the JWT exp.
    const accessToken = await signAccessToken({ sub: user.id, role: user.role });

    logger.info({ data: { id: user.id, email: user.email } }, "user logged in");

    return {
        user: {
            userId: user.id,
            email: user.email,
            fullName: user.fullName,
            loyaltyTier: user.loyaltyTier,
        },
        tokens: { accessToken, expiresIn: ACCESS_TOKEN_TTL_SECONDS },
    };
}
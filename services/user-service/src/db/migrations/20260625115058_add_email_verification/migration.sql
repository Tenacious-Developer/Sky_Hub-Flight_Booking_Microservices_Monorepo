-- AlterTable
ALTER TABLE "users" ADD COLUMN     "email_verify_attempts" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "email_verify_expires_at" TIMESTAMP(3),
ADD COLUMN     "email_verify_token" TEXT;

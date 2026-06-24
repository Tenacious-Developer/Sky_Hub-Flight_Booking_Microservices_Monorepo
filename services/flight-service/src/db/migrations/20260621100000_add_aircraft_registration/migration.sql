-- AlterTable
ALTER TABLE "aircrafts" ADD COLUMN "registration" VARCHAR(10) NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "aircrafts_registration_key" ON "aircrafts"("registration");

-- CreateTable
CREATE TABLE "aircrafts" (
    "id" TEXT NOT NULL,
    "model" VARCHAR(100) NOT NULL,
    "total_capacity" INTEGER NOT NULL,

    CONSTRAINT "aircrafts_pkey" PRIMARY KEY ("id")
);

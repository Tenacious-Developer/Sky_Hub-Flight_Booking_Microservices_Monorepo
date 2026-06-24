import { Prisma } from './../db/generated/prisma/client';
import { prisma } from "../config";
import { mapPrismaError } from "../db/prisma-error.mapper";
import { CreateAirportDTO, Airport, ListAirportsQueryDTO, UpdateAirportDTO } from "../dto/airport.dto";

export async function createAirport(input: CreateAirportDTO): Promise<Airport> {
    try {
        return await prisma.airport.create({ data: input });
    } catch (err) {
        throw mapPrismaError(err); // Prisma error → AppError (P2002 → 409, etc.)
    }
}

export async function getAllAirports(filters: ListAirportsQueryDTO): Promise<Airport[]> {
    try {
        const where: Prisma.AirportWhereInput = { isActive: true }; // hide soft-deleted

        if (filters.country) {
            where.country = { equals: filters.country, mode: "insensitive" };
        }

        if (filters.search) {
            where.OR = [
                { name: { contains: filters.search, mode: "insensitive" } },
                { city: { contains: filters.search, mode: "insensitive" } },
                { code: { contains: filters.search, mode: "insensitive" } },
            ];
        }

        // Reference data is small & bounded, so no full pagination — just cap the
        // result size to keep the autocomplete payload snappy.
        return await prisma.airport.findMany({ where, orderBy: { code: "asc" }, take: 50 });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

export async function getAirportByCode(code: string): Promise<Airport | null> {
    try {
        return await prisma.airport.findFirst({ where: { code, isActive: true } }); // soft-deleted → treated as not found
    } catch (err) {
        throw mapPrismaError(err);
    }
}

export async function updateAirport(code: string, data: UpdateAirportDTO): Promise<Airport> {
    try {
        return await prisma.airport.update({ where: { code }, data });
    } catch (err) {
        throw mapPrismaError(err);   // P2025 (not found) → 404, automatically
    }
}

// Soft delete: flip isActive instead of removing the row, so schedules that
// reference this airport (origin/destination) stay intact. P2025 (no such code) → 404.
export async function deactivateAirport(code: string): Promise<void> {
    try {
        await prisma.airport.update({ where: { code }, data: { isActive: false } });
    } catch (err) {
        throw mapPrismaError(err);
    }
}
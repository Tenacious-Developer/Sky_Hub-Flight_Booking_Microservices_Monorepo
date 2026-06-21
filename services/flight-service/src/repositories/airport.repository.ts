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
        const where: Prisma.AirportWhereInput = {};

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

        return await prisma.airport.findMany({ where, orderBy: { code: "asc" } });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

export async function getAirportByCode(code: string): Promise<Airport | null> {
    try {
        return await prisma.airport.findUnique({ where: { code } });
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

export async function deleteAirport(code: string): Promise<void> {
    try {
        await prisma.airport.delete({ where: { code } });
    } catch (err) {
        throw mapPrismaError(err);   // P2025 → 404, P2003 (referenced by flights) → 409
    }
}
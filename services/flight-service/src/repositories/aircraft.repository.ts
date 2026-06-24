import { prisma } from "../config";
import { Prisma } from "../db/generated/prisma/client";
import { mapPrismaError } from "../db/prisma-error.mapper";
import { CreateAircraftDTO, Aircraft, ListAircraftQueryDTO, UpdateAircraftDTO} from '../dto/aircraft.dto';


export async function createAircraft(aircraftInput:CreateAircraftDTO): Promise<Aircraft> {
    try {
        return await prisma.aircraft.create({data: aircraftInput})
    } catch (err) {
        throw mapPrismaError(err)
    }
}

export async function getAllAircrafts(filters: ListAircraftQueryDTO): Promise<Aircraft[]> {
    try {
        const where: Prisma.AircraftWhereInput = { isActive: true }; // hide soft-deleted

        if (filters.search) {
            where.OR = [
                { model: { contains: filters.search, mode: "insensitive" } },
                { registration: { contains: filters.search, mode: "insensitive" } },
            ];
        }

        if (filters.minCapacity || filters.maxCapacity) {
            where.totalCapacity = {
                ...(filters.minCapacity && { gte: filters.minCapacity }),
                ...(filters.maxCapacity && { lte: filters.maxCapacity }),
            };
        }

        // Reference data is small & bounded, so no full pagination — just cap the
        // result size to keep the admin/autocomplete payload snappy.
        return await prisma.aircraft.findMany({ where, orderBy: { registration: "asc" }, take: 50 });
    } catch (err) {
        throw mapPrismaError(err);
    }
}

export async function getAircraftById(id: string): Promise<Aircraft | null> {
    try {
        return await prisma.aircraft.findFirst({ where: { id, isActive: true } }); // soft-deleted → treated as not found
    } catch (err) {
        throw mapPrismaError(err);
    }
}

export async function updateAircraft(id: string, data: UpdateAircraftDTO): Promise<Aircraft> {
    try {
        return await prisma.aircraft.update({ where: { id }, data });
    } catch (err) {
        throw mapPrismaError(err);   // P2025 (not found) → 404, P2002 (dup registration) → 409
    }
}

// Soft delete: flip isActive instead of removing the row, so schedules/instances
// that reference this aircraft stay intact. P2025 (no such id) → 404.
export async function deactivateAircraft(id: string): Promise<void> {
    try {
        await prisma.aircraft.update({ where: { id }, data: { isActive: false } });
    } catch (err) {
        throw mapPrismaError(err);
    }
}
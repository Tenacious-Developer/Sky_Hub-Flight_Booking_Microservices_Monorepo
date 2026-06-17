import { prisma } from "../config";
import { mapPrismaError } from "../db/prisma-error.mapper";
import { CreateAirportDTO, Airport } from "../dto/airport.dto";

export async function createAirport(input: CreateAirportDTO): Promise<Airport> {
    try {
        return await prisma.airport.create({ data: input });
    } catch (err) {
        throw mapPrismaError(err); // Prisma error → AppError (P2002 → 409, etc.)
    }
}


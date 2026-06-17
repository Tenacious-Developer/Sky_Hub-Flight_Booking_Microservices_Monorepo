import { prisma } from "../config";
import { Prisma, Airport } from "../db/generated/prisma/client";
import { mapPrismaError } from "../db/prisma-error.mapper";

export async function createAirport(airportInput: Prisma.AirportCreateInput): Promise<Airport> {
    try {
        return await prisma.airport.create({ data: airportInput });
    } catch (err) {
        throw mapPrismaError(err); // Prisma error → AppError (P2002 → 409, etc.)
    }
}


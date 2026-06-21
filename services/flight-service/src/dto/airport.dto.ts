import { z } from "zod";
import { airportParamSchema, createAirportSchema, listAirportsQuerySchema, updateAirportSchema } from "../validators/airport.validator";

// (a) what the repo RECEIVES — request shape derived from Zod. No Prisma.
export type CreateAirportDTO = z.infer<typeof createAirportSchema>;

// (b) the domain airport the repo RETURNS — owned by us, NOT Prisma's model.
//     Keeps Prisma sealed inside the repository body (ORM-swappable).
export type Airport = {
    id: string;
    code: string;
    name: string;
    city: string;
    country: string;
    timezone: string;
};

// (c) what the CLIENT sees — domain Airport minus internal `id`.
export type ResponseAirportDTO = Omit<Airport, "id">;

/**
 * Single source of truth for "domain Airport → client response".
 * Builds a NEW object with an explicit allowlist of fields (no `id`), so the id
 * is actually removed at runtime — not just hidden by the type. If the domain
 * type gains an internal column later, it can never leak unless added here.
 */
export function toAirportResponse(airport: Airport): ResponseAirportDTO {
    return {
        code: airport.code,
        name: airport.name,
        city: airport.city,
        country: airport.country,
        timezone: airport.timezone,
    };
}

export type ListAirportsQueryDTO = z.infer<typeof listAirportsQuerySchema>;

export type AirportParamsDTO = z.infer<typeof airportParamSchema>;

export type UpdateAirportDTO = z.infer<typeof updateAirportSchema>;
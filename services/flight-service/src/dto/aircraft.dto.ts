import {z} from "zod"
import { aircraftParamSchema, createAircraftSchema, listAircraftQuerySchema, updateAircraftSchema } from "../validators/aircraft.validator"

export type CreateAircraftDTO = z.infer<typeof createAircraftSchema>

export type Aircraft = {
    id: string;
    model: string;
    totalCapacity: number;
    registration: string;
};

export type ResponseAircraftDTO = Aircraft;

/**
 * Single source of truth for "domain Aircraft → client response".
 * Builds a NEW object with an explicit allowlist of fields, so if the domain
 * type ever gains an internal column it can't leak unless added here.
 */
export function toAircraftResponse(aircraft: Aircraft): ResponseAircraftDTO {
    return {
        id: aircraft.id,
        model: aircraft.model,
        registration: aircraft.registration,
        totalCapacity: aircraft.totalCapacity,
    };
}


export type ListAircraftQueryDTO = z.infer<typeof listAircraftQuerySchema>; // renamed PascalCase
export type AircraftParamsDTO = z.infer<typeof aircraftParamSchema>;
export type UpdateAircraftDTO = z.infer<typeof updateAircraftSchema>;
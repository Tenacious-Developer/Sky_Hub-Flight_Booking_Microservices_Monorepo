import { z } from "zod";
import { createAirportSchema } from "../validators/airport.validator";

export type CreateAirportDTO = z.infer<typeof createAirportSchema>;

export type ResponseAirportDTO = {
    id: string;
    code: string;
    name: string;
    city: string;
    country: string;
    timezone: string;
};
import { z } from "zod";

export const createAirportSchema = z.object({
    code: z.string().length(3, "Airport code must be 3 characters long").toUpperCase(),
    name: z.string().min(3, "Airport name must be at least 3 characters long").max(150, "Airport name must be at most 150 characters long"),
    city: z.string().min(3, "City name must be at least 3 characters long").max(100, "City name must be at most 100 characters long"),
    country: z.string().min(3, "Country name must be at least 3 characters long").max(100, "Country name must be at most 100 characters long"),
    timezone: z.string().min(3, "Timezone must be at least 3 characters long").max(100, "Timezone must be at most 100 characters long"),
});



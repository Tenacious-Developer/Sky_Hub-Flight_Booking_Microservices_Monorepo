import { z } from "zod";

export const createAirportSchema = z.object({
    code: z
        .string()
        .trim()
        .length(3, "Airport code must be exactly 3 characters long")
        .regex(/^[a-zA-Z]{3}$/, "Airport code must be 3 letters (IATA)")
        .toUpperCase(),
    name: z.string().trim().min(2, "Airport name must be at least 2 characters long").max(150, "Airport name must be at most 150 characters long"),
    city: z.string().trim().min(2, "City name must be at least 2 characters long").max(100, "City name must be at most 100 characters long"),
    country: z.string().trim().min(2, "Country name must be at least 2 characters long").max(100, "Country name must be at most 100 characters long"),
    timezone: z.string().trim().min(5, "Timezone must be at least 5 characters long").max(100, "Timezone must be at most 100 characters long"),
});


export const listAirportsQuerySchema = z.object({
    country: z.string().trim().min(1).optional(),
    search: z.string().trim().min(1).optional(),
});

export const airportParamSchema = z.object({
    code: z.string()
        .trim()
        .length(3, "Airport code must be exactly 3 characters long")
        .regex(/^[a-zA-Z]{3}$/, "Airport code must be 3 letters (IATA)")
        .toUpperCase(),
})

export const updateAirportSchema = createAirportSchema
    .omit({ code: true })
    .partial()
    .refine((obj) => Object.keys(obj).length > 0, {
        message: "At least one field must be provided to update",
    });



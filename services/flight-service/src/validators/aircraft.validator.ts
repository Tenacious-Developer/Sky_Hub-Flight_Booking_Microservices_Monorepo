import { z } from "zod";

export const createAircraftSchema = z.object({
    model: z
        .string()
        .trim()
        .min(2, "Aircraft model must be at least 2 characters long")
        .max(100, "Aircraft model must be at most 100 characters long"),   // matches VarChar(100)

    registration: z
        .string()
        .trim()
        .toUpperCase()                                                     // normalize FIRST so the checks below see the uppercased value
        .min(2, "Registration must be at least 2 characters long")
        .max(10, "Registration must be at most 10 characters long")        // matches VarChar(10)
        .regex(/^[A-Z0-9-]+$/, "Registration must be letters, digits or hyphens (e.g. VT-ABC)"),

    totalCapacity: z
        .number({ message: "Total capacity must be a number" })
        .int("Total capacity must be a whole number")
        .positive("Total capacity must be greater than 0")
        .max(1000, "Total capacity must be at most 1000"),                  // sanity cap
});


export const listAircraftQuerySchema = z.object({
    search: z.string().trim().min(1).optional(),
    minCapacity: z.coerce.number().int().positive().optional(),
    maxCapacity: z.coerce.number().int().positive().optional(),
}).refine(
    (q) => !q.minCapacity || !q.maxCapacity || q.minCapacity <= q.maxCapacity,
    { message: "minCapacity must be less than or equal to maxCapacity" },
);


export const aircraftParamSchema = z.object({
    id: z.string().uuid("Aircraft id must be a valid UUID"),
});

export const updateAircraftSchema = createAircraftSchema
    .partial()
    .refine((obj) => Object.keys(obj).length > 0, {
        message: "At least one field must be provided to update",
    });

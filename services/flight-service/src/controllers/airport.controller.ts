import { Request, Response } from "express";
import { created } from "@skyhub/common-utils";
import { createAirportService } from "../services/airport.service";
import { CreateAirportDTO } from "../dto/airport.dto";

export const airportHandler = async (
    req: Request<unknown, unknown, CreateAirportDTO>,
    res: Response,
): Promise<void> => {
    const airport = await createAirportService(req.body);
    created(res, airport);
};




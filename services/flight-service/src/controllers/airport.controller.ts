import { Request, Response } from "express"
import { created } from "@skyhub/common-utils"
import { createAirportService } from "../services/airport.service"

export const airportHandler = async (req: Request, res: Response) => {
    const airport = await createAirportService(req.body);
    created(res, airport);
}




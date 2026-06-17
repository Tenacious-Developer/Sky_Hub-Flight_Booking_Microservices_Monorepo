import { logger } from "@skyhub/common-utils";
import { createAirport } from "../repositories/airport.repository";
import { CreateAirportDTO, ResponseAirportDTO, toAirportResponse } from "../dto/airport.dto";

export async function createAirportService(input: CreateAirportDTO): Promise<ResponseAirportDTO> {
    const airport = await createAirport(input);
    logger.info({ data: { id: airport.id, code: airport.code } }, "airport created");
    return toAirportResponse(airport);
}



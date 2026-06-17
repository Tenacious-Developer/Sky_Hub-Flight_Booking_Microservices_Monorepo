import { logger } from "@skyhub/common-utils";
import { createAirport } from "../repositories/airport.repository";
import { CreateAirportDTO, ResponseAirportDTO } from "../dto/airport.dto";

export async function createAirportService(airportInput: CreateAirportDTO): Promise<ResponseAirportDTO> {
    const airport = await createAirport(airportInput);
    logger.info({ data: { id: airport.id, code: airport.code } }, "airport created");
    return airport;
}



import { logger, NotFoundError } from "@skyhub/common-utils";
import { createAirport, getAllAirports, getAirportByCode, updateAirport, deleteAirport } from "../repositories/airport.repository";
import { CreateAirportDTO, AirportParamsDTO, ListAirportsQueryDTO, ResponseAirportDTO, toAirportResponse, UpdateAirportDTO } from "../dto/airport.dto";

export async function createAirportService(input: CreateAirportDTO): Promise<ResponseAirportDTO> {
    const airport = await createAirport(input);
    logger.info({ data: { id: airport.id, code: airport.code } }, "airport created");
    return toAirportResponse(airport);
}

export async function getAllAirportsService(filters: ListAirportsQueryDTO): Promise<ResponseAirportDTO[]> {
    const airports = await getAllAirports(filters);
    logger.info({ data: { count: airports.length } }, "airports fetched");
    return airports.map(toAirportResponse);
}

export async function getAirportService(param: AirportParamsDTO): Promise<ResponseAirportDTO> {
    const code = param.code.toUpperCase();              // normalize (params transform didn't persist)
    const airport = await getAirportByCode(code);
    if (!airport) {
        throw new NotFoundError(`Airport with code ${code} not found`);   // null → clean 404
    }
    logger.info({ data: { id: airport.id, code: airport.code } }, "airport fetched by code");
    return toAirportResponse(airport);
}

export async function updateAirportService(
    param: AirportParamsDTO,
    input: UpdateAirportDTO,
): Promise<ResponseAirportDTO> {
    const code = param.code.toUpperCase();          // same normalization as get-by-code
    const airport = await updateAirport(code, input);
    logger.info({ data: { id: airport.id, code: airport.code } }, "airport updated");
    return toAirportResponse(airport);
}

export async function deleteAirportService(param: AirportParamsDTO): Promise<void> {
    const code = param.code.toUpperCase();          // same normalization as get-by-code
    await deleteAirport(code);
    logger.info({ data: { code } }, "airport deleted");
}


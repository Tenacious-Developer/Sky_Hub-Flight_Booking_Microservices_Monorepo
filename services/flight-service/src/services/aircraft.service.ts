import { logger, NotFoundError } from "@skyhub/common-utils";
import { AircraftParamsDTO, CreateAircraftDTO, ListAircraftQueryDTO, ResponseAircraftDTO, toAircraftResponse, UpdateAircraftDTO } from "../dto/aircraft.dto";
import {createAircraft, deactivateAircraft, getAircraftById, getAllAircrafts, updateAircraft} from "../repositories/aircraft.repository"

export async function createAircraftService(aircraftInput: CreateAircraftDTO):Promise<ResponseAircraftDTO> {
    const aircraft = await createAircraft(aircraftInput);
    logger.info({ data: { id: aircraft.id, registration: aircraft.registration } }, "aircraft created");
    return toAircraftResponse(aircraft);
}

export async function getAllAircraftService(filters: ListAircraftQueryDTO): Promise<ResponseAircraftDTO[]> {
    const aircrafts = await getAllAircrafts(filters);
    logger.info({ data: { count: aircrafts.length } }, "aircrafts fetched");
    return aircrafts.map(toAircraftResponse);
}

export async function getAircraftService(param: AircraftParamsDTO): Promise<ResponseAircraftDTO> {
    const aircraft = await getAircraftById(param.id);
    if (!aircraft) {
        throw new NotFoundError(`Aircraft with id ${param.id} not found`);
    }
    logger.info({ data: { id: aircraft.id, registration: aircraft.registration } }, "aircraft fetched by id");
    return toAircraftResponse(aircraft);
}

export async function updateAircraftService(
    parm:AircraftParamsDTO, 
    input: UpdateAircraftDTO
): Promise<ResponseAircraftDTO> {
    const aircraft = await updateAircraft(parm.id, input);
    logger.info({ data: { id: aircraft.id, registration: aircraft.registration } }, "aircraft updated");
    return toAircraftResponse(aircraft);
}

export async function deactivateAircraftService(param: AircraftParamsDTO): Promise<void> {
    await deactivateAircraft(param.id);
    logger.info({ data: { id: param.id } }, "aircraft deactivated");
}
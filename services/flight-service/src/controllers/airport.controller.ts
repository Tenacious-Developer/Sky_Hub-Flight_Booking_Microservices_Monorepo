import { Request, Response } from "express";
import { created, ok, okMessage } from "@skyhub/common-utils";
import { createAirportService, getAllAirportsService, getAirportService, updateAirportService, deleteAirportService } from "../services/airport.service";
import { CreateAirportDTO, ListAirportsQueryDTO, UpdateAirportDTO } from "../dto/airport.dto";

export const createAirportHandler = async (
    req: Request<unknown, unknown, CreateAirportDTO>,
    res: Response,
): Promise<void> => {
    const airport = await createAirportService(req.body);
    created(res, airport, "Airport created successfully.");
};

export const getAllAirportsHandler = async (
    req: Request<unknown, unknown, unknown, ListAirportsQueryDTO>, 
    res: Response
): Promise<void> => {
    const airports = await getAllAirportsService(req.query);
    ok(res, airports);
};

export const getAirportHandler = async (
    req: Request<{code: string}>,
    res: Response,
): Promise<void> => {
    const airport = await getAirportService(req.params);
    ok(res, airport);
};


export const updateAirportHandler = async (
    req: Request<{ code: string }, unknown, UpdateAirportDTO>,
    res: Response,
): Promise<void> => {
    const airport = await updateAirportService(req.params, req.body);
    ok(res, airport);
};

export const deleteAirportHandler = async (
    req: Request<{ code: string }>,
    res: Response,
): Promise<void> => {
    await deleteAirportService(req.params);
    okMessage(res, "Airport deleted successfully.");
};

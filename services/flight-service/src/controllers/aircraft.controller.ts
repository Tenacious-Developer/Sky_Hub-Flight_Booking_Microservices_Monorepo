import { Request, Response} from "express";
import {createAircraftService, deactivateAircraftService, getAircraftService, getAllAircraftService, updateAircraftService} from "../services/aircraft.service"
import { CreateAircraftDTO, ListAircraftQueryDTO, UpdateAircraftDTO } from "../dto/aircraft.dto";
import { created, ok, okMessage } from "@skyhub/common-utils";

export const createAircraftHandler = async (
    req: Request<unknown, unknown, CreateAircraftDTO>, 
    res: Response,
): Promise<void> => {
    const aircraftResponse = await createAircraftService(req.body);
    created(res, aircraftResponse, "Aircraft Created Successfully.");

};

export const getAllAircraftHandler = async (
    req: Request<unknown, unknown, unknown, ListAircraftQueryDTO>, 
    res: Response,
): Promise<void> => {
    const aircraft = await getAllAircraftService(req.query);
    ok(res,aircraft);
};

export const getAircraftHandler = async (
    req: Request<{ id: string }>,
    res: Response,
): Promise<void> => {
    const aircraft = await getAircraftService(req.params);
    ok(res, aircraft);
};

export const updateAircraftHandler = async (
    req: Request<{ id: string }, unknown, UpdateAircraftDTO>,
    res: Response,
): Promise<void> => {
    const aircraft = await updateAircraftService(req.params, req.body);
    ok(res, aircraft);
};

export const deactivateAircraftHandler = async (
    req: Request<{ id: string }>,
    res: Response,
): Promise<void> => {
    await deactivateAircraftService(req.params);
    okMessage(res, "Aircraft deactivated successfully.");
};

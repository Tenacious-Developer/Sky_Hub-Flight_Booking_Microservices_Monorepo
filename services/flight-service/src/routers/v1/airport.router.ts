import { Router } from "express";
import { airportHandler } from "../../controllers/airport.controller";
import { validateRequest } from "@skyhub/common-utils";
import { createAirportSchema } from "../../validators/airport.validator";

const airportRouter = Router()

airportRouter.post('/', validateRequest({ body: createAirportSchema }), airportHandler);

export default airportRouter
    
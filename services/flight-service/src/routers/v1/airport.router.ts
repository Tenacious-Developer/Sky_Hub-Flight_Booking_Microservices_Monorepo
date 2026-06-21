import { Router } from "express";
import { createAirportHandler, getAllAirportsHandler, getAirportHandler, updateAirportHandler, deleteAirportHandler } from "../../controllers/airport.controller";
import { validateRequest } from "@skyhub/common-utils";
import { airportParamSchema, createAirportSchema, listAirportsQuerySchema, updateAirportSchema } from "../../validators/airport.validator";

const airportRouter = Router()

airportRouter.post('/', validateRequest({ body: createAirportSchema }), createAirportHandler);
airportRouter.get('/', validateRequest({query: listAirportsQuerySchema}), getAllAirportsHandler);
airportRouter.get('/:code', validateRequest({params: airportParamSchema}), getAirportHandler);
airportRouter.patch('/:code', validateRequest({ params: airportParamSchema, body: updateAirportSchema }), updateAirportHandler);
airportRouter.delete('/:code', validateRequest({ params: airportParamSchema }), deleteAirportHandler);

export default airportRouter
    
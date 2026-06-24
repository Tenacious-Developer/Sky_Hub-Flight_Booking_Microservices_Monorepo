import {Router} from "express";
import {createAircraftHandler, deactivateAircraftHandler, getAircraftHandler, getAllAircraftHandler, updateAircraftHandler} from "../../controllers/aircraft.controller"
import { validateRequest } from "@skyhub/common-utils";
import { aircraftParamSchema, createAircraftSchema, listAircraftQuerySchema, updateAircraftSchema } from "../../validators/aircraft.validator";


const aircraftRouter = Router();

aircraftRouter.post('/', validateRequest({body: createAircraftSchema}), createAircraftHandler);
aircraftRouter.get("/", validateRequest({ query: listAircraftQuerySchema }), getAllAircraftHandler);
aircraftRouter.get("/:id", validateRequest({ params: aircraftParamSchema }), getAircraftHandler);
aircraftRouter.patch("/:id", validateRequest({ params: aircraftParamSchema, body: updateAircraftSchema }), updateAircraftHandler);
aircraftRouter.delete("/:id", validateRequest({ params: aircraftParamSchema }), deactivateAircraftHandler);

export default aircraftRouter;

import { Router } from "express";
import healthRouter from "./health.router";
import airportRouter from "./airport.router";
import aircraftRouter from "./aircraft.router";

const v1Router = Router();

v1Router.use('/health', healthRouter);
v1Router.use('/airports', airportRouter);
v1Router.use('/aircrafts', aircraftRouter);

export default v1Router;    
import express from "express";
import {Config} from './config/index';
import router from "./routers/index.router";

const app = express();

app.use(express.json());

app.use('/api', router);

app.listen(Config.server.port, ()=>{
    console.log(`Flight Service is running on port ${Config.server.port}`)
})


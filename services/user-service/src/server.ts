import express from 'express';
import { Config } from './config/index.js';
import router from './routers/index.router';
import { globalErrorHandler, notFoundHandler } from '@skyhub/common-utils';


const app = express();

app.use(express.json());

app.use('/api', router);

app.use(notFoundHandler);    // catches unmatched routes — must come after all routes
app.use(globalErrorHandler); // must be last and must be 4-arg

app.listen(Config.server.port, () => {
  console.log(`User Service running on port ${Config.server.port}`);
});

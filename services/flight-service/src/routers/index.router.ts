import { Router } from "express";
import v1Router from "./v1/index";
import v2Router from "./v2/index";

const router = Router();

router.use('/v1', v1Router);
router.use('/v2', v2Router);

export default router;  

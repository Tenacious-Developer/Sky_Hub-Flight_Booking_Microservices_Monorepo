import {Router} from 'express';
import v1Router from './v1/index.router';
import v2Router from './v2/index.router';

const router = Router();

router.use('/v1', v1Router);
router.use('/v2', v2Router);

export default router;
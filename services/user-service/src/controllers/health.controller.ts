import { NextFunction, Request, Response } from "express";
import { AppError } from "@skyhub/common-utils";


const healthHandler = (req: Request, res: Response, next: NextFunction) => {
    return res.status(200).json({ service: "User-Service", status: "Live" })
    // throw new AppError(400, "This is a simple 400 Bad Request!");
}

export default healthHandler;


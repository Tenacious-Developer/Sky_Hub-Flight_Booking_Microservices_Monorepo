import { Request, Response, NextFunction } from "express"

export const healthHandler = (req:Request, res:Response, next:NextFunction) =>{
    return res.status(200).json({ service: "Flight-Service", status: "Live" })
}


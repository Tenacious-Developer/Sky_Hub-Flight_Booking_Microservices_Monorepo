import { env } from "./env";

type ServerConfig = {
    readonly port: number
}

export const serverConfig: ServerConfig = {
    port: env.PORT,
}

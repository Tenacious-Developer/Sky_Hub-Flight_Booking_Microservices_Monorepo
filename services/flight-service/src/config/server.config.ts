type ServerConfig = {
    readonly port: number
}

export const serverConfig: ServerConfig = {
    port: Number(process.env.PORT) || 3001,
}
import { env } from './env'
import { serverConfig } from './server.config'
import { prisma } from './client'

export const Config = {
    env,
    server: serverConfig,
} as const

export { prisma, env };

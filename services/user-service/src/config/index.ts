import { env } from './env'
import { serverConfig } from './server.config'
import { prisma } from './client'
import { loadKeys, getPrivateKey, getPublicKey } from './keys'

export const Config = {
    env,
    server: serverConfig,
} as const

export { prisma, env, loadKeys, getPrivateKey, getPublicKey };
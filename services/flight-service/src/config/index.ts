import 'dotenv/config'
import { serverConfig } from './server.config.js'

export const Config = {
    server: serverConfig,
} as const

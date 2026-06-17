import "dotenv/config";
import { defineConfig } from "prisma/config";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig({
  // Since the config is in src/config, the schema is one level up and inside db/
  schema: path.join(__dirname, "../db/schema.prisma"),
  migrations: {
    path: path.join(__dirname, "../db/migrations"),
  },
  datasource: {
    url: process.env["DATABASE_URL"],
  },
});


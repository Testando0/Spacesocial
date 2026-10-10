import { databaseReady, server } from "./server";

const rawPort = process.env["PORT"];

if (!rawPort) {
  throw new Error("PORT environment variable is required but was not provided.");
}

const port = Number(rawPort);

if (Number.isNaN(port) || port <= 0) {
  throw new Error(`Invalid PORT value: "${rawPort}"`);
}

await databaseReady;

server.listen(port, () => {
  console.log(`[REDSPACE] Server running on port ${port}`);
});

const { createServer } = require("net");
const { createServer: createTlsServer } = require("tls");
const fs = require("fs");
const path = require("path");
const PORT = 1883;
const TLS_PORT = 8883;
const TLS_OPTIONS = {
  key: fs.readFileSync(path.join(__dirname, "certs", "server.key")),
  cert: fs.readFileSync(path.join(__dirname, "certs", "server.crt")),
};

// Dynamic import for aedes rather than a top-level require: aedes resolves
// as an ESM-only package on some npm/registry combinations (this broke the
// EC2 deployment target running Node 18 with ERR_REQUIRE_ESM), so importing
// it lazily inside an async function keeps this file portable across
// Node versions.
async function main() {
  const { Aedes } = await import("aedes");
  const aedes = await Aedes.createBroker();
  const server = createServer(aedes.handle);
  server.listen(PORT, () => console.log(`[broker] listening on ${PORT} (plain TCP)`));

  const tlsServer = createTlsServer(TLS_OPTIONS, aedes.handle);
  tlsServer.listen(TLS_PORT, () => console.log(`[broker] listening on ${TLS_PORT} (TLS)`));
  aedes.on("client", (c) => console.log(`[broker] client connected: ${c.id}`));
  aedes.on("publish", (packet, client) => {
    if (client && !packet.topic.startsWith("$SYS")) console.log(`[broker] ${client.id} -> ${packet.topic}`);
  });
}
main();

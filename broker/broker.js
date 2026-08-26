const { createServer } = require("net");
const PORT = 1883;

// Dynamic import for aedes rather than a top-level require: aedes resolves
// as an ESM-only package on some npm/registry combinations (this broke the
// EC2 deployment target running Node 18 with ERR_REQUIRE_ESM), so importing
// it lazily inside an async function keeps this file portable across
// Node versions.
async function main() {
  const { Aedes } = await import("aedes");
  const aedes = await Aedes.createBroker();
  const server = createServer(aedes.handle);
  server.listen(PORT, () => console.log(`[broker] listening on ${PORT}`));
  aedes.on("client", (c) => console.log(`[broker] client connected: ${c.id}`));
  aedes.on("publish", (packet, client) => {
    if (client && !packet.topic.startsWith("$SYS")) console.log(`[broker] ${client.id} -> ${packet.topic}`);
  });
}
main();

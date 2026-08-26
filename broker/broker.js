const { Aedes } = require("aedes");
const { createServer } = require("net");
const PORT = 1883;

async function main() {
  const aedes = await Aedes.createBroker();
  const server = createServer(aedes.handle);
  server.listen(PORT, () => console.log(`[broker] listening on ${PORT}`));
  aedes.on("client", (c) => console.log(`[broker] client connected: ${c.id}`));
  aedes.on("publish", (packet, client) => {
    if (client && !packet.topic.startsWith("$SYS")) console.log(`[broker] ${client.id} -> ${packet.topic}`);
  });
}
main();

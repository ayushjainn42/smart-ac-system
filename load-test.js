const mqtt = require("mqtt");
const NUM_ZONES = 50; // bump to 200 later to match your plan's "worst case" scenario
const client = mqtt.connect("mqtt://localhost:1883", { clientId: "load-test" });
const pending = new Map();
const latencies = [];

client.on("connect", () => {
  client.subscribe("actuators/+/command");
  setTimeout(() => {
    console.log(`[load-test] firing burst: ${NUM_ZONES} zones publishing within the same second`);
    for (let i = 1; i <= NUM_ZONES; i++) {
      const zoneId = `z-burst-${i}`;
      const publishedAt = Date.now();
      pending.set(zoneId, publishedAt);
      client.publish(`sensors/${zoneId}/data`, JSON.stringify({
        zone_id: zoneId, timestamp: new Date(publishedAt).toISOString(),
        temp: 20 + Math.random() * 6, humidity: 50, occupied: Math.random() > 0.5,
      }));
    }
  }, 500);
});

client.on("message", (topic, payload) => {
  const msg = JSON.parse(payload.toString());
  const publishedAt = pending.get(msg.zone_id);
  if (publishedAt) {
    latencies.push(Date.now() - publishedAt);
    pending.delete(msg.zone_id);
    if (pending.size === 0) {
      const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      console.log(`[load-test] all ${NUM_ZONES} zones responded. avg latency=${avg.toFixed(1)}ms, max latency=${Math.max(...latencies)}ms`);
      process.exit(0);
    }
  }
});

setTimeout(() => {
  console.log(`[load-test] timeout — ${pending.size} zones never responded (possible bottleneck)`);
  process.exit(1);
}, 10000);

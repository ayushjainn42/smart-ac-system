const mqtt = require("mqtt");
const client = mqtt.connect("mqtt://localhost:1883", { clientId: "actuator-bank" });
const last = {};
client.on("connect", () => { console.log("[actuator] connected"); client.subscribe("actuators/+/command"); });
client.on("message", (topic, payload) => {
  const msg = JSON.parse(payload.toString());
  if (last[msg.zone_id] !== msg.command) { console.log(`[actuator:${msg.zone_id}] ${last[msg.zone_id] || "unknown"} -> ${msg.command}`); last[msg.zone_id] = msg.command; }
});

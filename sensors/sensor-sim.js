const mqtt = require("mqtt");
const zoneId = process.argv[2];
const zones = require("../config/zones.js");
const zoneConfig = zones.find((z) => z.zone_id === zoneId);
if (!zoneConfig) { console.error("Unknown zone"); process.exit(1); }

const client = mqtt.connect("mqtt://localhost:1883", { clientId: `sensor-${zoneId}` });
let temp = zoneConfig.temp_setpoint + (Math.random() * 2 - 1);
let humidity = 45 + Math.random() * 10;
let occupied = Math.random() > 0.5, counter = 0;

client.on("connect", () => {
  console.log(`[sensor:${zoneId}] connected`);
  setInterval(() => {
    temp = Math.max(15, Math.min(30, temp + (Math.random() - 0.5) * 0.4));
    humidity = Math.max(20, Math.min(70, humidity + (Math.random() - 0.5) * 1.5));
    counter++;
    if (counter > 4 + Math.random() * 6) { occupied = Math.random() > 0.4; counter = 0; }
    const reading = { zone_id: zoneId, timestamp: new Date().toISOString(), temp: +temp.toFixed(2), humidity: +humidity.toFixed(1), occupied };
    client.publish(`sensors/${zoneId}/data`, JSON.stringify(reading));
    console.log(`[sensor:${zoneId}] published`, reading);
  }, 3000);
});

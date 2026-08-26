const mqtt = require("mqtt");
const express = require("express");
const Database = require("better-sqlite3");
const zones = require("../config/zones.js");

const db = new Database("./logs/readings.db");
db.exec(`CREATE TABLE IF NOT EXISTS readings (id INTEGER PRIMARY KEY AUTOINCREMENT, zone_id TEXT, timestamp TEXT, temp REAL, humidity REAL, occupied INTEGER, actuator_state TEXT);
CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, zone_id TEXT, type TEXT, triggered_at TEXT, resolved_at TEXT);`);
const insertReading = db.prepare(`INSERT INTO readings (zone_id,timestamp,temp,humidity,occupied,actuator_state) VALUES (@zone_id,@timestamp,@temp,@humidity,@occupied,@actuator_state)`);
const insertAlert = db.prepare(`INSERT INTO alerts (zone_id,type,triggered_at) VALUES (?, 'zone_fault', ?)`);
const resolveAlert = db.prepare(`UPDATE alerts SET resolved_at=? WHERE zone_id=? AND resolved_at IS NULL`);

const zoneState = {};
zones.forEach((z) => { zoneState[z.zone_id] = { ...z, actuatorState: "idle", faulted: false, lastMessageAt: null }; });

function decide(cfg, r) {
  const tol = r.occupied ? cfg.tolerance : cfg.tolerance + cfg.occupancy_relax;
  if (r.temp > cfg.temp_setpoint + tol) return "cooling_on";
  if (r.temp < cfg.temp_setpoint - tol) return "heating_on";
  return r.occupied ? "idle" : "eco";
}

const client = mqtt.connect("mqtt://localhost:1883", { clientId: "zone-service" });
client.on("connect", () => { console.log("[zone-service] connected"); client.subscribe("sensors/+/data"); });
client.on("message", (topic, payload) => {
  const r = JSON.parse(payload.toString());
  const cfg = zones.find((z) => z.zone_id === r.zone_id);
  if (!cfg) return;
  const state = zoneState[r.zone_id];
  if (state.faulted) { state.faulted = false; resolveAlert.run(new Date().toISOString(), r.zone_id); console.log(`[zone-service] ${r.zone_id} recovered`); }
  state.lastMessageAt = Date.now();
  const actuatorState = decide(cfg, r);
  state.actuatorState = actuatorState;
  client.publish(`actuators/${r.zone_id}/command`, JSON.stringify({ zone_id: r.zone_id, command: actuatorState }));
  insertReading.run({ zone_id: r.zone_id, timestamp: r.timestamp, temp: r.temp, humidity: r.humidity, occupied: r.occupied ? 1 : 0, actuator_state: actuatorState });
  console.log(`[zone-service] ${r.zone_id} temp=${r.temp} occ=${r.occupied} -> ${actuatorState}`);
});

setInterval(() => {
  zones.forEach((z) => {
    const s = zoneState[z.zone_id];
    if (s.lastMessageAt && Date.now() - s.lastMessageAt > z.fault_timeout_ms && !s.faulted) {
      s.faulted = true;
      insertAlert.run(z.zone_id, new Date().toISOString());
      console.warn(`[zone-service] FAULT: ${z.zone_id} silent`);
    }
  });
}, 5000);

const app = express();
app.get("/zones/status", (req, res) => res.json(Object.values(zoneState)));
app.get("/alerts", (req, res) => res.json(db.prepare("SELECT * FROM alerts ORDER BY id DESC LIMIT 20").all()));
app.listen(4000, () => console.log("[zone-service] REST API on :4000"));

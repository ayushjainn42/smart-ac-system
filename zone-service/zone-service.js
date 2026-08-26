const mqtt = require("mqtt");
const express = require("express");
const path = require("path");
const Database = require("better-sqlite3");
const zones = require("../config/zones.js");

const DEFAULT_ZONE_CFG = { temp_setpoint: 22.0, tolerance: 1.0, occupancy_relax: 2.0, fault_timeout_ms: 15000, name: "Unconfigured zone" };

const db = new Database("./logs/readings.db");
db.exec(`CREATE TABLE IF NOT EXISTS readings (id INTEGER PRIMARY KEY AUTOINCREMENT, zone_id TEXT, timestamp TEXT, temp REAL, humidity REAL, occupied INTEGER, actuator_state TEXT, latency_ms INTEGER);
CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, zone_id TEXT, type TEXT, triggered_at TEXT, resolved_at TEXT);`);
const insertReading = db.prepare(`INSERT INTO readings (zone_id,timestamp,temp,humidity,occupied,actuator_state,latency_ms) VALUES (@zone_id,@timestamp,@temp,@humidity,@occupied,@actuator_state,@latency_ms)`);
const insertAlert = db.prepare(`INSERT INTO alerts (zone_id,type,triggered_at) VALUES (?, 'zone_fault', ?)`);
const resolveAlert = db.prepare(`UPDATE alerts SET resolved_at=? WHERE zone_id=? AND resolved_at IS NULL`);

const zoneState = {};
zones.forEach((z) => { zoneState[z.zone_id] = { ...z, actuatorState: "idle", faulted: false, lastMessageAt: null, lastReading: null }; });

function getConfig(zoneId) {
  return zones.find((z) => z.zone_id === zoneId) || { zone_id: zoneId, ...DEFAULT_ZONE_CFG };
}

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
  const cfg = getConfig(r.zone_id);

  if (!zoneState[r.zone_id]) zoneState[r.zone_id] = { ...cfg, actuatorState: "idle", faulted: false, lastMessageAt: null, lastReading: null };
  const state = zoneState[r.zone_id];
  if (state.faulted) { state.faulted = false; resolveAlert.run(new Date().toISOString(), r.zone_id); console.log(`[zone-service] ${r.zone_id} recovered`); }
  state.lastMessageAt = Date.now();
  state.lastReading = r;

  const actuatorState = decide(cfg, r);
  state.actuatorState = actuatorState;
  const latencyMs = Date.now() - new Date(r.timestamp).getTime();

  client.publish(`actuators/${r.zone_id}/command`, JSON.stringify({ zone_id: r.zone_id, command: actuatorState, timestamp: new Date().toISOString() }));
  insertReading.run({ zone_id: r.zone_id, timestamp: r.timestamp, temp: r.temp, humidity: r.humidity, occupied: r.occupied ? 1 : 0, actuator_state: actuatorState, latency_ms: latencyMs });
  console.log(`[zone-service] ${r.zone_id} temp=${r.temp} occ=${r.occupied} -> ${actuatorState} (latency ${latencyMs}ms)`);
});

setInterval(() => {
  Object.keys(zoneState).forEach((zoneId) => {
    const s = zoneState[zoneId];
    const timeout = s.fault_timeout_ms || DEFAULT_ZONE_CFG.fault_timeout_ms;
    if (s.lastMessageAt && Date.now() - s.lastMessageAt > timeout && !s.faulted) {
      s.faulted = true;
      insertAlert.run(zoneId, new Date().toISOString());
      console.warn(`[zone-service] FAULT: ${zoneId} silent`);
    }
  });
}, 5000);

const app = express();
app.use(express.static(path.join(__dirname, "..", "public")));
app.get("/zones/status", (req, res) => res.json(Object.values(zoneState)));
app.get("/alerts", (req, res) => res.json(db.prepare("SELECT * FROM alerts ORDER BY id DESC LIMIT 20").all()));
app.get("/stats/latency", (req, res) => {
  res.json(db.prepare("SELECT AVG(latency_ms) as avg_ms, MAX(latency_ms) as max_ms, COUNT(*) as n FROM readings WHERE latency_ms IS NOT NULL").get());
});
app.listen(4000, () => console.log("[zone-service] REST API + dashboard on :4000"));

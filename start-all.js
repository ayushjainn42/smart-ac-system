const { spawn } = require("child_process");
const path = require("path");
const zones = require("./config/zones.js");

const procs = [];
function launch(name, scriptPath, args = []) {
  const p = spawn("node", [scriptPath, ...args], { stdio: "inherit" });
  p.on("exit", (code) => console.log(`[launcher] ${name} exited with code ${code}`));
  procs.push(p);
}

console.log("[launcher] starting broker...");
launch("broker", path.join(__dirname, "broker", "broker.js"));

setTimeout(() => {
  launch("zone-service", path.join(__dirname, "zone-service", "zone-service.js"));
  launch("actuator", path.join(__dirname, "actuator", "actuator-sim.js"));
  zones.forEach((z) => launch(`sensor-${z.zone_id}`, path.join(__dirname, "sensors", "sensor-sim.js"), [z.zone_id]));
}, 1500);

process.on("SIGINT", () => { procs.forEach((p) => p.kill()); process.exit(0); });

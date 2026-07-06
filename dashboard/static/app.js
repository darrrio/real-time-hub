const zonesEl = document.getElementById("zones");
const outdoorEl = document.getElementById("outdoor");
const statusEl = document.getElementById("conn-status");

const charts = {}; // zone_id -> Chart instance
const cards = {}; // zone_id -> { root, tempNow, tempSetpoint, actionBadge, suggestion, updatedAt }

function fmtTemp(v) {
  return v === null || v === undefined ? "—" : `${v.toFixed(1)}°C`;
}

function fmtTime(iso) {
  if (!iso) return "never";
  const d = new Date(iso);
  return d.toLocaleTimeString();
}

function actionClass(action) {
  return ["heat", "cool", "hold"].includes(action) ? action : "unknown";
}

function createZoneCard(zoneId) {
  const root = document.createElement("div");
  root.className = "zone-card";
  root.innerHTML = `
    <h2>${zoneId.replace(/_/g, " ")}</h2>
    <div class="zone-metrics">
      <div>
        <div class="temp-now">—</div>
        <div class="temp-setpoint">setpoint —</div>
      </div>
      <span class="action-badge unknown">—</span>
    </div>
    <canvas></canvas>
    <div class="suggestion empty">No suggestion yet.</div>
    <div class="updated-at">never updated</div>
  `;
  zonesEl.appendChild(root);

  const canvas = root.querySelector("canvas");
  const chart = new Chart(canvas, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Temperature",
          data: [],
          borderColor: "#e0693e",
          tension: 0.3,
          pointRadius: 0,
        },
        {
          label: "Setpoint",
          data: [],
          borderColor: "#3e8ee0",
          borderDash: [4, 4],
          tension: 0,
          pointRadius: 0,
        },
        {
          label: "High Boundary",
          data: [],
          borderColor: "#e40eec",
          borderDash: [4, 4],
          tension: 0,
          pointRadius: 0,
        },
        {
          label: "Low Boundary",
          data: [],
          borderColor: "#02e459",
          borderDash: [4, 4],
          tension: 0,
          pointRadius: 0,
        },
      ],
    },
    options: {
      animation: false,
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: false },
        y: { ticks: { callback: (v) => `${v}°` } },
      },
    },
  });

  charts[zoneId] = chart;
  cards[zoneId] = {
    root,
    tempNow: root.querySelector(".temp-now"),
    tempSetpoint: root.querySelector(".temp-setpoint"),
    actionBadge: root.querySelector(".action-badge"),
    suggestion: root.querySelector(".suggestion"),
    updatedAt: root.querySelector(".updated-at"),
  };
  return cards[zoneId];
}

function renderZone(zone) {
  const card = cards[zone.zone_id] || createZoneCard(zone.zone_id);

  card.tempNow.textContent = fmtTemp(zone.temperature_c);
  card.tempSetpoint.textContent = `setpoint ${fmtTemp(zone.setpoint_c)}`;

  const action = zone.action || "unknown";
  card.actionBadge.textContent = action;
  card.actionBadge.className = `action-badge ${actionClass(action)}`;

  if (zone.suggestion) {
    card.suggestion.textContent = zone.suggestion;
    card.suggestion.classList.remove("empty");
  } else {
    card.suggestion.textContent = "No suggestion yet (zone is within comfort range).";
    card.suggestion.classList.add("empty");
  }

  card.updatedAt.textContent = `updated ${fmtTime(zone.updated_at)}`;

  const chart = charts[zone.zone_id];
  const history = zone.history || [];
  chart.data.labels = history.map((h) => fmtTime(h.t));
  chart.data.datasets[0].data = history.map((h) => h.temperature_c);
  chart.data.datasets[1].data = history.map((h) => h.setpoint_c);
  chart.data.datasets[2].data = history.map((h) => h.setpoint_c + 0.5);
  chart.data.datasets[3].data = history.map((h) => h.setpoint_c - 0.5);
  chart.update("none");
}

function renderOutdoor(outdoor) {
  if (outdoor && outdoor.temperature_c !== undefined) {
    outdoorEl.textContent = `Outdoor: ${fmtTemp(outdoor.temperature_c)}`;
  }
}

function setConnected(connected) {
  statusEl.textContent = connected ? "live" : "reconnecting…";
  statusEl.className = `status ${connected ? "connected" : "disconnected"}`;
}

async function loadInitialState() {
  try {
    const res = await fetch("/api/state");
    const state = await res.json();
    renderOutdoor(state.outdoor);
    state.zones.forEach(renderZone);
  } catch (err) {
    console.error("failed to load initial state", err);
  }
}

function connectWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${protocol}://${window.location.host}/ws`);

  ws.onopen = () => setConnected(true);
  ws.onclose = () => {
    setConnected(false);
    setTimeout(connectWebSocket, 2000);
  };
  ws.onerror = () => ws.close();

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "zone") {
      renderZone(msg.data);
    } else if (msg.type === "outdoor") {
      renderOutdoor(msg.data);
    }
  };
}

loadInitialState().then(connectWebSocket);

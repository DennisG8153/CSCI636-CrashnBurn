#!/usr/bin/env python3
"""
generate_dashboard.py
─────────────────────
Run from mr3/ directory:  python generate_dashboard.py
Outputs:                  dashboard.html  (self-contained, no server needed)

Reads:
  mr_daily_final.csv    — zone+date level crash+store join
  mr_hourly_final.csv   — zone+hour level crash+store join
  daily_crash_mr.csv    — all crash zones (incl. crash-only, no store)
"""

import json, os, sys
import pandas as pd
import numpy as np
from collections import defaultdict

# ── Paths ─────────────────────────────────────────────────────────────────────
DAILY_FINAL  = "mr_daily_final.csv"
HOURLY_FINAL = "mr_hourly_final.csv"
DAILY_MR     = "daily_crash_mr.csv"
OUTPUT       = "index.html"

# ── Data builders ─────────────────────────────────────────────────────────────

def build_static(df: pd.DataFrame) -> dict:
    """Per-zone all-time totals."""
    agg = df.groupby("zone_id").agg(
        crashes           = ("crashes",           "sum"),
        injured           = ("injured",           "sum"),
        killed            = ("killed",            "sum"),
        store_count       = ("store_count",       "first"),
        active_licenses   = ("active_licenses",   "first"),
        outdated_licenses = ("outdated_licenses", "first"),
        type_counts_json  = ("type_counts_json",  "first"),
        lat               = ("crash_lat",         "mean"),
        lon               = ("crash_lon",         "mean"),
    ).reset_index()

    out = {}
    for _, r in agg.iterrows():
        try:   types = json.loads(r["type_counts_json"])
        except: types = {}
        out[r["zone_id"]] = {
            "c":   int(r["crashes"]),
            "i":   int(r["injured"]),
            "k":   int(r["killed"]),
            "s":   int(r["store_count"]),
            "a":   int(r["active_licenses"]),
            "o":   int(r["outdated_licenses"]),
            "t":   types,
            "lat": round(float(r["lat"]), 6),
            "lon": round(float(r["lon"]), 6),
        }
    return out


def build_daily_by_date(df: pd.DataFrame) -> dict:
    """{ date → { zone_id → {c, i, k, lat, lon} } } for daily map view."""
    out = defaultdict(dict)
    for _, r in df.iterrows():
        d = str(r["crash_date"])[:10]
        out[d][r["zone_id"]] = {
            "c":   int(r["crashes"]),
            "i":   int(r["injured"]),
            "k":   int(r["killed"]),
            "lat": round(float(r["crash_lat"]), 6),
            "lon": round(float(r["crash_lon"]), 6),
        }
    return dict(out)


def build_daily_timeline(df: pd.DataFrame) -> dict:
    """{ zone_id → [{d, c, i, k}, ...] } for sidebar timeline chart."""
    out = defaultdict(list)
    for _, r in df.sort_values("crash_date").iterrows():
        out[r["zone_id"]].append({
            "d": str(r["crash_date"])[:10],
            "c": int(r["crashes"]),
            "i": int(r["injured"]),
            "k": int(r["killed"]),
        })
    return dict(out)


def build_hourly_by_hour(df: pd.DataFrame) -> dict:
    """{ hour → { zone_id → {ac, ai, ak, lat, lon, s, a, o} } } for hourly map view."""
    # Build per-zone static lookup for store enrichment
    zone_store = {}
    for _, r in df.drop_duplicates("zone_id").iterrows():
        zone_store[r["zone_id"]] = {
            "s": int(r["store_count"]),
            "a": int(r["active_licenses"]),
            "o": int(r["outdated_licenses"]),
        }

    out = defaultdict(dict)
    for _, r in df.iterrows():
        h   = int(r["hour"])
        zid = r["zone_id"]
        sv  = zone_store.get(zid, {"s": 0, "a": 0, "o": 0})
        out[h][zid] = {
            "ac":  round(float(r["avg_crashes"]), 4),
            "ai":  round(float(r["avg_injured"]), 4),
            "ak":  round(float(r["avg_killed"]),  4),
            "lat": round(float(r["crash_lat"]),   6),
            "lon": round(float(r["crash_lon"]),   6),
            **sv,
        }
    return {int(k): v for k, v in out.items()}


def build_zone_hourly(df: pd.DataFrame) -> dict:
    """{ zone_id → [24 x {ac, ai, ak}] } for sidebar hourly bar chart."""
    empty = [{"ac": 0, "ai": 0, "ak": 0} for _ in range(24)]
    out   = {}
    for _, r in df.iterrows():
        zid = r["zone_id"]
        if zid not in out:
            out[zid] = [dict(x) for x in empty]
        h = int(r["hour"])
        out[zid][h] = {
            "ac": round(float(r["avg_crashes"]), 4),
            "ai": round(float(r["avg_injured"]), 4),
            "ak": round(float(r["avg_killed"]),  4),
        }
    return out


def build_crash_only(df_daily_mr: pd.DataFrame, daily_zones: set) -> dict:
    """Zones with crashes but no nearby stores (from daily_crash_mr only)."""
    df_daily_mr["crashes"] = pd.to_numeric(df_daily_mr.get("crashes"), errors="coerce").fillna(0)
    agg = df_daily_mr.groupby("zone_id").agg(
        crashes = ("crashes", "sum"),
        lat     = ("avg_lat", "mean"),
        lon     = ("avg_lon", "mean"),
    ).reset_index()

    out = {}
    for _, r in agg.iterrows():
        if r["zone_id"] not in daily_zones:
            out[r["zone_id"]] = {
                "c":   int(r["crashes"]),
                "lat": round(float(r["lat"]), 6),
                "lon": round(float(r["lon"]), 6),
            }
    return out


# ── HTML template ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>(Don't) Crash &amp; Burn — Manhattan</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg:       #080c10;
  --surf:     #0e1419;
  --surf2:    #141b22;
  --surf3:    #1a2330;
  --border:   #1f2d3d;
  --border2:  #2a3d52;
  --text:     #cdd9e5;
  --muted:    #5c7080;
  --dim:      #3b4d5e;
  --accent:   #ff5c00;
  --accent2:  #ff8c42;
  --red:      #f44747;
  --orange:   #ff8c42;
  --yellow:   #f5c842;
  --blue:     #3b82f6;
  --green:    #3fb950;
  --font-ui:  'DM Sans', sans-serif;
  --font-mono:'Space Mono', monospace;
}
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);font-family:var(--font-ui);font-size:13px}

/* ── Layout ── */
#app{display:flex;flex-direction:column;height:100vh}
#main{flex:1;display:flex;overflow:hidden;position:relative}

/* ── Header ── */
header{
  background:var(--surf);
  border-bottom:1px solid var(--border);
  padding:0 18px;
  height:52px;
  display:flex;
  align-items:center;
  gap:20px;
  flex-shrink:0;
  z-index:1000;
}
.brand{
  font-family:var(--font-mono);
  font-size:13px;
  font-weight:700;
  color:var(--accent);
  white-space:nowrap;
  letter-spacing:-0.3px;
}
.brand-sub{
  font-family:var(--font-ui);
  font-size:11px;
  color:var(--muted);
  font-weight:400;
  margin-left:8px;
  letter-spacing:0;
}

/* Mode tabs */
.tabs{display:flex;gap:2px;background:var(--surf2);padding:3px;border-radius:8px;border:1px solid var(--border)}
.tab{
  padding:5px 16px;
  border:none;
  background:transparent;
  color:var(--muted);
  border-radius:6px;
  cursor:pointer;
  font-size:12px;
  font-weight:500;
  font-family:var(--font-ui);
  transition:all 0.15s;
  white-space:nowrap;
}
.tab:hover{color:var(--text)}
.tab.active{background:var(--accent);color:#fff;font-weight:600}

/* Header right */
.legend{
  margin-left:auto;
  display:flex;
  align-items:center;
  gap:12px;
  font-size:11px;
  color:var(--muted);
}
.leg{display:flex;align-items:center;gap:5px}
.leg-dot{width:9px;height:9px;border-radius:2px;flex-shrink:0}

.hdr-stats{
  display:flex;
  gap:16px;
  padding-left:16px;
  border-left:1px solid var(--border);
  font-size:11px;
  color:var(--muted);
  white-space:nowrap;
}
.hv{color:var(--text);font-weight:600;font-family:var(--font-mono);font-size:12px}

/* ── Map ── */
#map{flex:1}
.leaflet-container{background:#080c10 !important}
.leaflet-control-zoom a{
  background:var(--surf2) !important;
  color:var(--text) !important;
  border-color:var(--border) !important;
}
.leaflet-control-zoom a:hover{background:var(--surf3) !important}

/* Leaflet tooltip override */
.ztip.leaflet-tooltip{
  background:var(--surf2) !important;
  border:1px solid var(--border2) !important;
  color:var(--text) !important;
  font-size:12px !important;
  font-family:var(--font-ui) !important;
  padding:7px 11px !important;
  border-radius:7px !important;
  box-shadow:0 4px 16px rgba(0,0,0,0.5) !important;
}
.ztip.leaflet-tooltip::before{display:none !important}

/* ── Sidebar ── */
#sidebar{
  width:0;
  background:var(--surf);
  border-left:1px solid var(--border);
  display:flex;
  flex-direction:column;
  overflow:hidden;
  flex-shrink:0;
  transition:width 0.2s ease;
}
#sidebar.open{width:310px}
.sb-head{
  padding:12px 14px;
  border-bottom:1px solid var(--border);
  display:flex;
  align-items:center;
  justify-content:space-between;
  flex-shrink:0;
}
.sb-title{
  font-size:10px;
  font-weight:600;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:1px;
  font-family:var(--font-mono);
}
.sb-close{
  background:none;border:none;color:var(--muted);
  cursor:pointer;font-size:18px;line-height:1;padding:0 2px;
  transition:color 0.1s;
}
.sb-close:hover{color:var(--text)}
#sb-body{
  flex:1;overflow-y:auto;padding:14px 14px 20px;
  scrollbar-width:thin;scrollbar-color:var(--border) transparent;
}
#sb-body::-webkit-scrollbar{width:3px}
#sb-body::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}

/* Sidebar content */
.zone-id{
  font-family:var(--font-mono);
  font-size:9px;
  color:var(--dim);
  margin-bottom:10px;
  word-break:break-all;
  letter-spacing:0.5px;
}
.stat-row{
  display:grid;
  grid-template-columns:1fr 1fr 1fr;
  gap:6px;
  margin-bottom:14px;
}
.scard{
  background:var(--surf2);
  border:1px solid var(--border);
  border-radius:7px;
  padding:9px 6px;
  text-align:center;
}
.scard-val{font-size:20px;font-weight:700;font-family:var(--font-mono);line-height:1}
.scard-lbl{font-size:10px;color:var(--muted);margin-top:3px}
.vc{color:var(--accent)}
.vi{color:var(--yellow)}
.vk{color:var(--red)}

.sec{
  font-size:10px;
  font-weight:600;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:1px;
  font-family:var(--font-mono);
  margin:14px 0 7px;
}
.store-box{
  background:var(--surf2);
  border:1px solid var(--border);
  border-radius:7px;
  padding:10px 12px;
  margin-bottom:2px;
}
.srow{display:flex;justify-content:space-between;align-items:center;padding:3px 0}
.srow:not(:last-child){border-bottom:1px solid var(--border)}
.srow-lbl{color:var(--muted);font-size:12px}
.srow-val{font-weight:600;font-size:12px}
.val-active{color:var(--green)}
.val-out{color:var(--dim)}

.type-list{list-style:none;margin-top:2px}
.type-item{
  display:flex;
  justify-content:space-between;
  align-items:center;
  padding:5px 0;
  border-bottom:1px solid var(--border);
  gap:8px;
}
.type-item:last-child{border:none}
.type-name{
  color:var(--muted);
  font-size:11px;
  overflow:hidden;
  text-overflow:ellipsis;
  white-space:nowrap;
  flex:1;
  max-width:210px;
}
.type-cnt{
  color:var(--accent);
  font-weight:700;
  font-size:12px;
  font-family:var(--font-mono);
  flex-shrink:0;
}

.chart-wrap{position:relative;height:140px;margin-top:4px}

.crash-only-tag{
  background:rgba(59,130,246,0.08);
  border:1px solid rgba(59,130,246,0.25);
  border-radius:6px;
  padding:7px 10px;
  font-size:11px;
  color:var(--blue);
  margin-bottom:12px;
  display:flex;
  align-items:center;
  gap:6px;
}

/* ── Bottom controls ── */
#ctrl-bar{
  background:var(--surf);
  border-top:1px solid var(--border);
  padding:10px 20px;
  display:none;
  align-items:center;
  gap:16px;
  flex-shrink:0;
}
#ctrl-bar.vis{display:flex}

#hour-lbl{
  font-family:var(--font-mono);
  font-size:13px;
  font-weight:700;
  color:var(--accent);
  min-width:76px;
}
.slider-wrap{flex:1;display:flex;flex-direction:column;gap:3px}
#hour-slider{
  width:100%;
  accent-color:var(--accent);
  cursor:pointer;
  height:4px;
}
.tick-row{
  display:flex;
  justify-content:space-between;
  font-size:10px;
  color:var(--dim);
  font-family:var(--font-mono);
}

#date-sel{
  background:var(--surf2);
  border:1px solid var(--border);
  color:var(--text);
  padding:5px 10px;
  border-radius:6px;
  font-size:12px;
  font-family:var(--font-ui);
  cursor:pointer;
  outline:none;
}
#date-sel:focus{border-color:var(--accent)}
.ctrl-hint{font-size:12px;color:var(--muted)}
#daily-zone-ct{font-size:12px;color:var(--text);font-weight:600;font-family:var(--font-mono)}

/* Sidebar empty state */
#sb-empty{
  height:100%;
  display:flex;
  flex-direction:column;
  align-items:center;
  justify-content:center;
  color:var(--dim);
  text-align:center;
  padding:24px;
  gap:10px;
}
.sb-empty-icon{font-size:28px;opacity:0.4}
.sb-empty-txt{font-size:12px;line-height:1.7;color:var(--muted)}
</style>
</head>
<body>
<div id="app">

  <header>
    <div>
      <span class="brand">(Don't) Crash &amp; Burn</span>
      <span class="brand-sub">Manhattan Alcohol-Involved Collisions</span>
    </div>
    <div class="tabs">
      <button class="tab active" onclick="setMode('static')">Static</button>
      <button class="tab"        onclick="setMode('daily')">Daily</button>
      <button class="tab"        onclick="setMode('hourly')">Hourly Avg</button>
    </div>
    <div class="legend">
      <div class="leg"><div class="leg-dot" style="background:#f44747"></div>High</div>
      <div class="leg"><div class="leg-dot" style="background:#ff8c42"></div>Med</div>
      <div class="leg"><div class="leg-dot" style="background:#f5c842"></div>Low</div>
      <div class="leg"><div class="leg-dot" style="background:#3b82f6"></div>No stores</div>
    </div>
    <div class="hdr-stats">
      <div><span class="hv" id="s-zones">—</span> zones</div>
      <div><span class="hv" id="s-crashes">—</span> crashes</div>
      <div><span class="hv" id="s-injured">—</span> injured</div>
    </div>
  </header>

  <div id="main">
    <div id="map"></div>
    <div id="sidebar">
      <div class="sb-head">
        <span class="sb-title">Zone Detail</span>
        <button class="sb-close" onclick="closeSidebar()">×</button>
      </div>
      <div id="sb-body">
        <div id="sb-empty">
          <div class="sb-empty-icon">◎</div>
          <div class="sb-empty-txt">Click any zone on the map to view crash statistics and store details.</div>
        </div>
        <div id="sb-zone" style="display:none"></div>
      </div>
    </div>
  </div>

  <div id="ctrl-bar">
    <!-- Populated by setMode() -->
  </div>

</div>

<script>
// ════════════════════════════════════════════════════════
//  DATA  (injected by generate_dashboard.py)
// ════════════════════════════════════════════════════════
__DATA_INJECTION__

// ════════════════════════════════════════════════════════
//  STATE
// ════════════════════════════════════════════════════════
let mode        = 'static';
let curHour     = new Date().getHours();
let curDate     = DATES.length ? DATES[DATES.length - 1] : null;
let hexLayers   = {};
let selZone     = null;
let tChart      = null;
let hChart      = null;

// ════════════════════════════════════════════════════════
//  MAP INIT
// ════════════════════════════════════════════════════════
const map = L.map('map', {zoomControl:true, attributionControl:false})
  .setView([40.758, -73.985], 13);

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  subdomains:'abcd', maxZoom:19
}).addTo(map);

// ════════════════════════════════════════════════════════
//  COLOR SCALE
// ════════════════════════════════════════════════════════
function scale(ratio) {
  if (ratio > 0.80) return ['#f44747', 0.80];
  if (ratio > 0.60) return ['#ff5c00', 0.70];
  if (ratio > 0.40) return ['#ff8c42', 0.60];
  if (ratio > 0.20) return ['#f5c842', 0.50];
  return ['#fff176', 0.38];
}

function maxOf(obj, key) {
  let m = 0;
  for (const v of Object.values(obj)) {
    const val = v[key];
    if (typeof val === 'number' && val > m) m = val;
  }
  return m || 1;
}

// ════════════════════════════════════════════════════════
//  HEX LAYER MANAGEMENT
// ════════════════════════════════════════════════════════
function clearHexes() {
  for (const p of Object.values(hexLayers)) map.removeLayer(p);
  hexLayers = {};
}

function drawLayer(zoneData, valueKey, maxV, blue) {
  let zones = 0, crashes = 0, injured = 0;

  for (const [zid, d] of Object.entries(zoneData)) {
    let boundary;
    try { boundary = h3.cellToBoundary(zid); }
    catch { continue; }

    const val   = d[valueKey] || 0;
    const ratio = val / maxV;
    const [color, fillOp] = blue ? ['#3b82f6', 0.32] : scale(ratio);

    const poly = L.polygon(boundary, {
      color, fillColor:color, fillOpacity:fillOp,
      weight:0.6, opacity:0.85,
    });

    poly.bindTooltip(buildTip(zid, d, blue), {
      sticky:true, className:'ztip', opacity:1,
    });
    poly.on('click', () => showDrill(zid, blue));
    poly.addTo(map);
    hexLayers[zid] = poly;

    zones++;
    crashes += (d.c ?? d.ac ?? 0);
    injured += (d.i ?? d.ai ?? 0);
  }
  return {zones, crashes, injured};
}

function updateStats(a, b) {
  const z = a.zones + b.zones;
  const c = Math.round(a.crashes + b.crashes);
  const i = Math.round(a.injured + b.injured);
  document.getElementById('s-zones').textContent   = z.toLocaleString();
  document.getElementById('s-crashes').textContent = c.toLocaleString();
  document.getElementById('s-injured').textContent = i.toLocaleString();
}

function buildTip(zid, d, blue) {
  if (blue) {
    return `<b>Crashes (alcohol):</b> ${d.c}<br>
            <span style="color:#3b82f6;font-size:11px">No liquor stores in this zone</span>`;
  }
  if (d.s !== undefined) {
    return `<b>Crashes:</b> ${d.c} &nbsp;·&nbsp; <b>Inj:</b> ${d.i} &nbsp;·&nbsp; <b>Killed:</b> ${d.k}<br>
            <b>Stores:</b> ${d.s} (${d.a} active / ${d.o} outdated)`;
  }
  return `<b>Avg crashes/day at this hour:</b> ${(d.ac||0).toFixed(2)}<br>
          <b>Stores:</b> ${d.s||0} (${d.a||0} active)`;
}

// ════════════════════════════════════════════════════════
//  RENDER MODES
// ════════════════════════════════════════════════════════
function renderStatic() {
  clearHexes();
  const maxC = maxOf(STATIC_DATA, 'c');
  const a = drawLayer(STATIC_DATA, 'c', maxC, false);
  const b = drawLayer(CRASH_ONLY,  'c', maxC, true);
  updateStats(a, b);
}

function renderDaily(date) {
  clearHexes();
  const dayData = DAILY_BY_DATE[date];
  if (!dayData) {
    document.getElementById('s-zones').textContent   = '0';
    document.getElementById('s-crashes').textContent = '0';
    document.getElementById('s-injured').textContent = '0';
    return;
  }

  // Enrich with store info from STATIC_DATA
  const enriched = {}, crashOnly = {};
  for (const [zid, d] of Object.entries(dayData)) {
    const st = STATIC_DATA[zid];
    if (st) enriched[zid] = {...d, s:st.s, a:st.a, o:st.o, t:st.t};
    else    crashOnly[zid] = d;
  }

  const maxC = maxOf(enriched, 'c') || maxOf(crashOnly, 'c') || 1;
  const a = drawLayer(enriched,  'c', maxC, false);
  const b = drawLayer(crashOnly, 'c', maxC, true);
  updateStats(a, b);

  const ct = document.getElementById('daily-zone-ct');
  if (ct) ct.textContent = `${Object.keys(dayData).length} zones`;
}

function renderHourly(hour) {
  clearHexes();
  const hourData = HOURLY_BY_HOUR[hour] || {};
  const maxC = maxOf(hourData, 'ac');
  const a = drawLayer(hourData, 'ac', maxC, false);
  updateStats(a, {zones:0, crashes:0, injured:0});

  const h12  = hour % 12 || 12;
  const ampm = hour < 12 ? 'AM' : 'PM';
  const lbl  = document.getElementById('hour-lbl');
  if (lbl) lbl.textContent = `${h12}:00 ${ampm}`;
}

// ════════════════════════════════════════════════════════
//  MODE SWITCHING
// ════════════════════════════════════════════════════════
function setMode(m) {
  mode = m;
  document.querySelectorAll('.tab').forEach((b,i) => {
    b.classList.toggle('active', ['static','daily','hourly'][i] === m);
  });

  const ctrl = document.getElementById('ctrl-bar');
  ctrl.className = '';

  if (m === 'daily') {
    ctrl.className = 'vis';
    ctrl.innerHTML = `
      <span class="ctrl-hint">Date:</span>
      <select id="date-sel" onchange="onDateChange(this.value)">
        ${[...DATES].reverse().map(d=>`<option value="${d}">${d}</option>`).join('')}
      </select>
      <span class="ctrl-hint">showing crashes on selected date</span>
      <span id="daily-zone-ct" style="margin-left:auto"></span>`;
    if (curDate) { document.getElementById('date-sel').value = curDate; }
    renderDaily(curDate || DATES[DATES.length-1]);

  } else if (m === 'hourly') {
    ctrl.className = 'vis';
    const h12  = curHour % 12 || 12;
    const ampm = curHour < 12 ? 'AM' : 'PM';
    ctrl.innerHTML = `
      <span id="hour-lbl">${h12}:00 ${ampm}</span>
      <div class="slider-wrap">
        <input type="range" id="hour-slider" min="0" max="23" value="${curHour}"
               oninput="onHourChange(this.value)">
        <div class="tick-row">
          <span>12a</span><span>3a</span><span>6a</span><span>9a</span>
          <span>12p</span><span>3p</span><span>6p</span><span>9p</span><span>11p</span>
        </div>
      </div>`;
    renderHourly(curHour);

  } else {
    renderStatic();
  }

  closeSidebar();
}

function onDateChange(val) {
  curDate = val;
  renderDaily(val);
  if (selZone) showDrill(selZone);
}

function onHourChange(val) {
  curHour = parseInt(val);
  renderHourly(curHour);
  if (selZone) showDrill(selZone);
}

// ════════════════════════════════════════════════════════
//  DRILL-DOWN SIDEBAR
// ════════════════════════════════════════════════════════
function showDrill(zid, isCrashOnly) {
  selZone = zid;
  document.getElementById('sidebar').classList.add('open');
  document.getElementById('sb-empty').style.display  = 'none';
  const el = document.getElementById('sb-zone');
  el.style.display = 'block';

  // Highlight selected hex
  for (const [z, p] of Object.entries(hexLayers)) {
    p.setStyle({weight: z === zid ? 2.5 : 0.6, opacity: z === zid ? 1 : 0.85});
  }

  const sd     = STATIC_DATA[zid];
  const daily  = DAILY_DATA[zid]    || [];
  const hourly = ZONE_HOURLY[zid]   || Array(24).fill({ac:0});

  const totalC = sd ? sd.c : daily.reduce((s,d)=>s+d.c, 0);
  const totalI = sd ? sd.i : daily.reduce((s,d)=>s+d.i, 0);
  const totalK = sd ? sd.k : daily.reduce((s,d)=>s+d.k, 0);

  let html = `<div class="zone-id">${zid}</div>`;

  if (!sd || isCrashOnly) {
    html += `<div class="crash-only-tag">⚠ No liquor stores mapped to this zone</div>`;
  }

  html += `
    <div class="stat-row">
      <div class="scard"><div class="scard-val vc">${totalC}</div><div class="scard-lbl">Crashes</div></div>
      <div class="scard"><div class="scard-val vi">${totalI}</div><div class="scard-lbl">Injured</div></div>
      <div class="scard"><div class="scard-val vk">${totalK}</div><div class="scard-lbl">Killed</div></div>
    </div>`;

  if (sd) {
    html += `
      <div class="sec">Liquor Stores</div>
      <div class="store-box">
        <div class="srow"><span class="srow-lbl">Total stores</span><span class="srow-val">${sd.s}</span></div>
        <div class="srow"><span class="srow-lbl">Active licenses</span><span class="srow-val val-active">${sd.a}</span></div>
        <div class="srow"><span class="srow-lbl">Outdated licenses</span><span class="srow-val val-out">${sd.o}</span></div>
      </div>`;

    const types = Object.entries(sd.t || {}).sort((a,b)=>b[1]-a[1]);
    if (types.length) {
      html += `<div class="sec">Store Types</div><ul class="type-list">`;
      for (const [name, cnt] of types) {
        const short = name.length > 30 ? name.slice(0,28)+'…' : name;
        html += `<li class="type-item">
          <span class="type-name" title="${name}">${short}</span>
          <span class="type-cnt">${cnt}</span>
        </li>`;
      }
      html += `</ul>`;
    }
  }

  html += `
    <div class="sec">Crash Timeline</div>
    <div class="chart-wrap"><canvas id="tc"></canvas></div>
    <div class="sec">Avg Crashes by Hour</div>
    <div class="chart-wrap"><canvas id="hc"></canvas></div>`;

  el.innerHTML = html;

  // Chart defaults
  const GRID = '#141b22';
  const TICK = {color:'#5c7080', font:{size:10, family:'Space Mono'}};

  // Timeline chart
  if (tChart) tChart.destroy();
  tChart = new Chart(document.getElementById('tc').getContext('2d'), {
    type:'line',
    data:{
      labels: daily.map(d=>d.d),
      datasets:[{
        data:       daily.map(d=>d.c),
        borderColor:'#ff5c00',
        backgroundColor:'rgba(255,92,0,0.12)',
        borderWidth:1.5,
        pointRadius:2,
        pointBackgroundColor:'#ff5c00',
        tension:0.35,
        fill:true,
      }]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{ticks:{...TICK, maxTicksLimit:7}, grid:{color:GRID}},
        y:{ticks:TICK, grid:{color:GRID}, beginAtZero:true, min:0},
      }
    }
  });

  // Hourly bar chart
  if (hChart) hChart.destroy();
  const hLabels = Array.from({length:24},(_,i)=> i%3===0
    ? (i===0?'12a': i<12?`${i}a`: i===12?'12p':`${i-12}p`) : '');

  hChart = new Chart(document.getElementById('hc').getContext('2d'), {
    type:'bar',
    data:{
      labels: hLabels,
      datasets:[{
        data: hourly.map(h=>h.ac||0),
        backgroundColor: Array.from({length:24},(_,i)=>
          i===curHour ? '#ff4455' : 'rgba(255,140,66,0.65)'),
        borderWidth:0,
        borderRadius:2,
      }]
    },
    options:{
      responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{ticks:TICK, grid:{color:GRID}},
        y:{ticks:TICK, grid:{color:GRID}, beginAtZero:true},
      }
    }
  });
}

function closeSidebar() {
  selZone = null;
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sb-empty').style.display  = 'flex';
  document.getElementById('sb-zone').style.display   = 'none';
  for (const p of Object.values(hexLayers)) p.setStyle({weight:0.6, opacity:0.85});
}

// ════════════════════════════════════════════════════════
//  INIT
// ════════════════════════════════════════════════════════
renderStatic();
</script>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    for f in [DAILY_FINAL, HOURLY_FINAL, DAILY_MR]:
        if not os.path.exists(f):
            print(f"ERROR: {f} not found. Run MR notebooks first.")
            sys.exit(1)

    print("Loading CSVs...")
    df_daily   = pd.read_csv(DAILY_FINAL)
    df_hourly  = pd.read_csv(HOURLY_FINAL)
    df_daily_mr = pd.read_csv(DAILY_MR)

    # Normalise numeric cols
    for col in ["crashes","injured","killed","store_count","active_licenses","outdated_licenses"]:
        if col in df_daily.columns:
            df_daily[col] = pd.to_numeric(df_daily[col], errors="coerce").fillna(0)

    print("Building data structures...")
    static_data   = build_static(df_daily)
    daily_by_date = build_daily_by_date(df_daily)
    daily_data    = build_daily_timeline(df_daily)
    hourly_by_hr  = build_hourly_by_hour(df_hourly)
    zone_hourly   = build_zone_hourly(df_hourly)
    crash_only    = build_crash_only(df_daily_mr, set(static_data.keys()))
    dates         = sorted(df_daily["crash_date"].str[:10].unique().tolist())

    print(f"  Static zones    : {len(static_data):,}")
    print(f"  Crash-only zones: {len(crash_only):,}")
    print(f"  Date range      : {dates[0]} → {dates[-1]}  ({len(dates)} dates)")
    print(f"  Hours covered   : {sorted(hourly_by_hr.keys())}")

    # Build JS injection block
    js = "\n".join([
        f"const STATIC_DATA    = {json.dumps(static_data,   separators=(',',':'))};",
        f"const DAILY_BY_DATE  = {json.dumps(daily_by_date,  separators=(',',':'))};",
        f"const DAILY_DATA     = {json.dumps(daily_data,     separators=(',',':'))};",
        f"const HOURLY_BY_HOUR = {json.dumps({str(k):v for k,v in hourly_by_hr.items()}, separators=(',',':'))};",
        f"const ZONE_HOURLY    = {json.dumps(zone_hourly,    separators=(',',':'))};",
        f"const CRASH_ONLY     = {json.dumps(crash_only,     separators=(',',':'))};",
        f"const DATES          = {json.dumps(dates)};",
        # h3 key fix: JS uses string keys, convert HOURLY_BY_HOUR
        "const _HBH = {}; for (const [k,v] of Object.entries(HOURLY_BY_HOUR)) _HBH[+k]=v;",
        "Object.assign(HOURLY_BY_HOUR, _HBH);",
    ])

    html = HTML_TEMPLATE.replace("__DATA_INJECTION__", js)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT) / 1024
    print(f"\n✓ Saved → {OUTPUT}  ({size_kb:.0f} KB)")
    print("  Open in any browser — no server required.")


if __name__ == "__main__":
    main()

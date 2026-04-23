#!/usr/bin/env python3
"""gui.py — minimal Flask UI for the blog SEO longtail researcher.

Run:  python gui.py
Open: http://localhost:5000

Single file. ~30 MB RAM idle. ~110 MB peak when pipeline runs in background.
"""

import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, redirect, request

try:
    import markdown as md
except ImportError:
    md = None

from discover_seeds import discover_seeds  # noqa: E402

# Default competitor list — copied from the HabitaOne tenant config in
# ~/2tb/Backlinker Manager/tenants/habitaone/config.toml
DEFAULT_COMPETITORS = (
    "conlallave.com,tuinmueble.com.ve,rentahouse.com.ve,mercadopiso.com,"
    "mlscaracas.com,inmuebles.com.ve,rexup.net,tunuevoinmueble.com,"
    "remax.com.ve,century21.com.ve,inmuebles.mercadolibre.com.ve"
)

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "output"
# Prefer the local .venv for dev; fall back to the current interpreter (prod/container)
_venv_py = ROOT / ".venv" / "bin" / "python"
PYTHON = str(_venv_py) if _venv_py.exists() else sys.executable
BATCH = ROOT / "batch.py"

app = Flask(__name__)

RUN_STATE = {
    "running": False,
    "started_at": None,
    "stdout_lines": [],   # capped to last 200
    "exit_code": None,
    "duration": None,
    "args": None,
}
_LOCK = threading.Lock()


def _stream_pipeline(cmd):
    """Run the pipeline subprocess and tee stdout into RUN_STATE.

    Adds:
      - `python -u` to disable stdout buffering (so we see live progress)
      - PYTHONUNBUFFERED=1 env to defend against re-exec/child procs
      - try/finally so RUN_STATE always resets (no stuck running=True)
      - return-code sentinel (-1) if the subprocess fails to spawn at all
    """
    # Force unbuffered mode at the subprocess level
    if cmd and cmd[0].endswith("python") or (cmd and "/python" in cmd[0]):
        cmd = [cmd[0], "-u"] + cmd[1:]
    child_env = {**os.environ, "PYTHONUNBUFFERED": "1"}

    with _LOCK:
        RUN_STATE.update(
            running=True, started_at=datetime.now().isoformat(timespec="seconds"),
            stdout_lines=[], exit_code=None, duration=None, args=cmd,
        )
    start = time.time()
    returncode = -1
    try:
        proc = subprocess.Popen(
            cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=child_env,
        )
        for line in proc.stdout:
            line = line.rstrip()
            with _LOCK:
                RUN_STATE["stdout_lines"].append(line)
                if len(RUN_STATE["stdout_lines"]) > 200:
                    RUN_STATE["stdout_lines"] = RUN_STATE["stdout_lines"][-200:]
        proc.wait()
        returncode = proc.returncode
    except Exception as e:
        with _LOCK:
            RUN_STATE["stdout_lines"].append(f"ERROR launching pipeline: {e}")
    finally:
        with _LOCK:
            RUN_STATE.update(
                running=False, exit_code=returncode,
                duration=round(time.time() - start, 1),
            )


def _latest_shortlist():
    files = sorted(OUTPUT.glob("batch_shortlist_*.md"),
                   key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _list_shortlists(limit=20):
    files = sorted(OUTPUT.glob("batch_shortlist_*.md"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:limit]


def _render_md(text):
    if md is None:
        return f"<pre>{text}</pre>"
    return md.markdown(text, extensions=["fenced_code", "tables"])


SHARED_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght,SOFT@9..144,300..900,0..100&family=JetBrains+Mono:wght@400;500;700&display=swap');

:root {
  --paper: #efe9d9;
  --paper-light: #f5f0e2;
  --ink: #2c2418;
  --ink-soft: #4a3f2f;
  --accent: #8b1e3f;     /* oxblood */
  --gold: #b8893d;       /* matte gold */
  --taupe: #87796a;
  --rule: #2c2418;
  --fr: 'Fraunces', 'Times New Roman', serif;
  --mono: 'JetBrains Mono', 'Menlo', monospace;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

html, body {
  background: var(--paper);
  color: var(--ink);
  font-family: var(--mono);
  font-size: 13px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
  font-feature-settings: 'tnum' 1, 'liga' 1;
}

/* Subtle paper grain via SVG noise */
body::before {
  content: '';
  position: fixed; inset: 0;
  background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='180' height='180'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2'/><feColorMatrix values='0 0 0 0 0.17 0 0 0 0 0.14 0 0 0 0 0.10 0 0 0 0.04 0'/></filter><rect width='100%25' height='100%25' filter='url(%23n)'/></svg>");
  opacity: 0.55;
  pointer-events: none;
  z-index: 1;
  mix-blend-mode: multiply;
}

a { color: var(--accent); text-decoration: none; border-bottom: 1px solid currentColor; }
a:hover { color: var(--ink); }

/* === Top status ticker === */
.ticker {
  position: relative; z-index: 2;
  display: flex; justify-content: space-between; align-items: center;
  padding: 8px 24px;
  border-bottom: 1px solid var(--rule);
  font: 500 10.5px/1 var(--mono);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink-soft);
}
.ticker .left, .ticker .right { display: flex; gap: 22px; align-items: center; }
.ticker .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--gold); display: inline-block; vertical-align: 1px; margin-right: 4px; }
.ticker .dot.on { background: #4a8a3f; box-shadow: 0 0 6px #4a8a3f; }
.ticker .dot.warn { background: var(--accent); animation: pulse 1.4s ease-in-out infinite; }
@keyframes pulse { 50% { opacity: 0.4; } }

/* === Masthead === */
.masthead {
  position: relative; z-index: 2;
  border-bottom: 3px solid var(--rule);
  padding: 24px 24px 18px;
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: end;
  gap: 32px;
}
.wordmark {
  font: 800 88px/0.82 var(--fr);
  letter-spacing: -0.045em;
  color: var(--ink);
  font-variation-settings: 'opsz' 144, 'SOFT' 0;
}
.wordmark .punct { color: var(--accent); }
.dek {
  text-transform: uppercase;
  font: 500 10.5px/1.55 var(--mono);
  letter-spacing: 0.18em;
  color: var(--ink-soft);
  text-align: left;
  padding-bottom: 6px;
}
.dek .label { color: var(--taupe); }
.issue {
  text-align: right;
  font: 600 10.5px/1.4 var(--mono);
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--ink-soft);
  padding-bottom: 6px;
}

/* === Layout === */
main {
  position: relative; z-index: 2;
  display: grid;
  grid-template-columns: 360px 1fr;
  gap: 0;
  border-bottom: 1px solid var(--rule);
}
.col-left { border-right: 1px solid var(--rule); padding: 28px 24px 32px; }
.col-right { padding: 28px 32px 32px; min-width: 0; }

.section-title {
  font: 700 11px/1 var(--mono);
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--accent);
  padding-bottom: 10px;
  border-bottom: 2px solid var(--ink);
  margin-bottom: 18px;
  display: flex; justify-content: space-between; align-items: baseline;
}
.section-title .badge {
  font-size: 10px;
  color: var(--taupe);
  letter-spacing: 0.12em;
}

/* === Form: notebook style === */
.field { margin-bottom: 16px; }
.field label.lbl {
  display: block;
  font: 600 9.5px/1 var(--mono);
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
  margin-bottom: 6px;
}
.field label.lbl a { font-size: 9.5px; letter-spacing: 0.14em; }
.field input[type=text],
.field input[type=number],
.field input:not([type]),
.field textarea,
.field select {
  width: 100%;
  background: transparent;
  border: 0;
  border-bottom: 1.5px dashed var(--ink-soft);
  border-radius: 0;
  padding: 6px 0;
  font: 400 13px/1.4 var(--mono);
  color: var(--ink);
  outline: none;
}
.field textarea {
  border: 1.5px dashed var(--ink-soft);
  padding: 10px;
  min-height: 92px;
  resize: vertical;
}
.field input:focus, .field textarea:focus, .field select:focus {
  border-style: solid;
  border-color: var(--accent);
}
.field-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.field-row .field { margin-bottom: 0; }
.checkbox {
  display: flex; align-items: center; gap: 8px;
  margin: 6px 0;
  font: 500 11px/1.2 var(--mono);
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--ink-soft);
  cursor: pointer;
  user-select: none;
}
.checkbox input { appearance: none; width: 14px; height: 14px; border: 1.5px solid var(--ink); background: transparent; cursor: pointer; position: relative; }
.checkbox input:checked { background: var(--ink); }
.checkbox input:checked::after {
  content: '✓'; position: absolute; inset: 0;
  color: var(--paper); font-size: 11px; line-height: 11px;
  display: flex; align-items: center; justify-content: center;
}

button.primary, button.secondary {
  display: inline-block;
  font: 700 11px/1 var(--mono);
  letter-spacing: 0.16em;
  text-transform: uppercase;
  padding: 12px 22px;
  border: 1.5px solid var(--ink);
  background: var(--ink);
  color: var(--paper);
  cursor: pointer;
  transition: all 0.12s ease-out;
}
button.primary:hover, button.secondary:hover {
  background: var(--accent);
  border-color: var(--accent);
}
button.primary:disabled {
  background: transparent; color: var(--taupe); border-color: var(--taupe); cursor: not-allowed;
}
button.secondary {
  background: transparent;
  color: var(--ink);
}
button.secondary:hover { background: var(--ink); color: var(--paper); }

/* === Status block === */
.statusblock {
  margin: 18px 0 0;
  padding: 14px 16px;
  border: 1.5px solid var(--ink);
  background: var(--paper-light);
  font: 500 11px/1.4 var(--mono);
  letter-spacing: 0.06em;
}
.statusblock .head { font-weight: 700; text-transform: uppercase; letter-spacing: 0.16em; color: var(--ink); margin-bottom: 6px; font-size: 10px; }
.statusblock .head .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; vertical-align: 1px; margin-right: 6px; background: var(--taupe); }
.statusblock.idle .dot { background: var(--gold); }
.statusblock.running { border-color: var(--accent); }
.statusblock.running .dot { background: var(--accent); animation: pulse 1.2s ease-in-out infinite; }
.statusblock.done { background: #e6e3d2; }
.statusblock.done .dot { background: #4a8a3f; }
.statusblock.error { border-color: var(--accent); background: #f1dad8; }

.log {
  margin-top: 12px;
  background: var(--ink);
  color: #d6cdb6;
  padding: 12px 14px;
  font: 400 11px/1.5 var(--mono);
  height: 200px; overflow-y: auto;
  white-space: pre-wrap;
  border-left: 4px solid var(--gold);
}
.log:empty::before { content: 'awaiting output...'; color: #5e5544; font-style: italic; }

/* Past runs ledger */
.runs { font: 500 11px/1.4 var(--mono); }
.runs .run {
  display: flex; justify-content: space-between; gap: 12px;
  padding: 6px 0;
  border-bottom: 1px dotted var(--ink-soft);
  cursor: pointer;
  letter-spacing: 0.04em;
}
.runs .run:hover { color: var(--accent); }
.runs .run .when { color: var(--taupe); font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; }
.runs .empty { color: var(--taupe); font-style: italic; padding: 8px 0; }

/* === Results: editorial cards === */
.results-meta {
  display: flex; justify-content: space-between; align-items: baseline;
  font: 500 10.5px/1 var(--mono);
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--taupe);
  border-bottom: 1px solid var(--ink-soft);
  padding-bottom: 8px;
  margin-bottom: 24px;
}
.results-empty {
  padding: 60px 0;
  text-align: center;
  font-family: var(--fr);
  font-style: italic;
  font-size: 22px;
  color: var(--taupe);
}

/* Markdown rendering of the shortlist */
.results h1 {
  font: 800 36px/1 var(--fr);
  letter-spacing: -0.02em;
  font-variation-settings: 'opsz' 144;
  margin-bottom: 18px;
  border-bottom: 3px solid var(--ink);
  padding-bottom: 12px;
}
.results h2 {
  font: 600 16px/1 var(--mono);
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--accent);
  margin: 36px 0 14px;
  padding-bottom: 4px;
  border-bottom: 1px solid var(--ink);
}
.results h3 {
  font: 700 22px/1.15 var(--fr);
  letter-spacing: -0.015em;
  margin: 18px 0 8px;
  color: var(--ink);
}
.results h3 code {
  font: 600 12px/1 var(--mono);
  background: var(--accent);
  color: var(--paper);
  padding: 3px 7px;
  letter-spacing: 0.08em;
  vertical-align: 3px;
  margin-right: 8px;
}
.results p {
  margin: 6px 0 12px;
  max-width: 64ch;
}
.results code {
  font: 500 12px/1 var(--mono);
  background: var(--paper-light);
  border: 1px solid var(--ink-soft);
  padding: 1px 6px;
}
.results strong { color: var(--ink); font-weight: 700; }
.results ul {
  list-style: none;
  margin: 8px 0 16px;
  padding: 0;
}
.results ul li {
  padding: 4px 0 4px 20px;
  position: relative;
  border-bottom: 1px dotted var(--ink-soft);
}
.results ul li:last-child { border-bottom: 0; }
.results ul li::before {
  content: '§';
  position: absolute;
  left: 0;
  color: var(--gold);
  font-family: var(--fr);
  font-weight: 600;
  top: 4px;
}
.results details {
  margin: 14px 0;
  border: 1px solid var(--ink-soft);
  background: var(--paper-light);
  padding: 10px 14px;
}
.results details[open] { border-color: var(--ink); }
.results summary {
  cursor: pointer;
  font: 600 10.5px/1 var(--mono);
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--accent);
  list-style: none;
  position: relative;
  padding-left: 16px;
}
.results summary::marker { display: none; }
.results summary::before { content: '▸'; position: absolute; left: 0; transition: transform 0.15s; }
.results details[open] summary::before { transform: rotate(90deg); }
.results table {
  width: 100%;
  border-collapse: collapse;
  font: 500 12px/1.3 var(--mono);
  margin: 12px 0 18px;
}
.results th {
  text-align: left;
  background: var(--ink);
  color: var(--paper);
  padding: 8px 10px;
  font-size: 10px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
}
.results td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--ink-soft);
}
.results hr {
  border: 0;
  border-top: 2px solid var(--ink);
  margin: 28px 0;
}

/* Section "## N." style — render the cluster numbers as folio marks */
.results h2:not(:first-child) {
  display: grid;
  grid-template-columns: auto 1fr;
  align-items: center;
  gap: 14px;
}

/* Footer */
.footer {
  position: relative; z-index: 2;
  padding: 18px 24px;
  border-top: 1px solid var(--ink-soft);
  text-align: center;
  font: 500 9.5px/1.6 var(--mono);
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--taupe);
}

/* Util */
.tnum { font-feature-settings: 'tnum' 1; }
@media (max-width: 980px) {
  main { grid-template-columns: 1fr; }
  .col-left { border-right: 0; border-bottom: 1px solid var(--rule); }
  .wordmark { font-size: 56px; }
  .masthead { grid-template-columns: 1fr; gap: 8px; }
  .issue, .dek { text-align: left; }
}
"""

PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Bienes · SEO topic finder</title>
<style>""" + SHARED_CSS + """
.help { font: 400 11px/1.4 var(--mono); color: var(--taupe); margin-top: 4px;
        letter-spacing: 0.02em; text-transform: none; }
</style>
</head><body>

<div class="ticker">
  <div class="left">
    <span><span class="dot on"></span>Bienes</span>
    <span>SEO topic finder</span>
  </div>
  <div class="right" id="tickerStatus">
    <span><span class="dot"></span>READY</span>
    <span id="tickerLast">— · no recent run</span>
  </div>
</div>

<div class="masthead">
  <h1 class="wordmark">Bienes<span class="punct">.</span></h1>
  <div class="dek">
    <div class="label">— Spanish SEO topic finder —</div>
    Give it a few topic ideas;<br>
    it returns the best blog articles to write.
  </div>
  <div class="issue">
    HabitaOne / Caracas<br>
    Built 2026
  </div>
</div>

<main>
  <div class="col-left">

    <div class="section-title">
      <span>1. Run a search</span>
      <span class="badge">setup</span>
    </div>

    <form id="runform" onsubmit="return runPipeline(event)">
      <div class="field">
        <label class="lbl">Topics to research (one per line)
          <a href="/discover" style="float:right">› Auto-suggest topics</a>
        </label>
        <textarea name="seeds" id="seedsBox" required>comprar casa venezuela
vender casa venezuela
credito hipotecario venezuela
comprar casa venezuela desde el exterior</textarea>
        <div class="help">Short topic ideas — Bienes expands each into 50-200 long-tail variants automatically.</div>
      </div>

      <div class="field">
        <label class="lbl">Reddit communities to mine</label>
        <input name="subreddits" value="venezuela,vzla">
        <div class="help">Comma-separated, no "r/" prefix. Used to find what real users discuss.</div>
      </div>

      <div class="field-row">
        <div class="field">
          <label class="lbl">Countries (Google search)</label>
          <input name="locales" value="ve,co,us">
          <div class="help">2-letter codes. ve = Venezuela, co = Colombia, us = USA. Picks easiest country to rank in.</div>
        </div>
        <div class="field">
          <label class="lbl">Google checks</label>
          <input name="check_serp" type="number" value="20" min="0" max="100">
          <div class="help">How many top phrases to deeply analyze (0 = skip, fast but less accurate).</div>
        </div>
      </div>

      <div class="field-row">
        <div class="field">
          <label class="lbl">Article ideas to show</label>
          <input name="shortlist" type="number" value="15" min="1" max="50">
          <div class="help">Final count in the result (top picks).</div>
        </div>
        <div class="field">
          <label class="lbl">Auto-discover rounds</label>
          <input name="recurse" type="number" value="1" min="0" max="3">
          <div class="help">0 = use only your topics. 1 = also harvest related topics from Reddit + trends.</div>
        </div>
      </div>

      <label class="checkbox"><input type="checkbox" name="check_freshness" checked> Check how old the top pages are</label>
      <label class="checkbox"><input type="checkbox" name="diff_last"> Highlight what's new since last run</label>

      <button id="runbtn" class="primary" type="submit" style="margin-top: 14px">▶ Find article topics</button>
    </form>

    <div id="status" class="statusblock idle">
      <div class="head"><span class="dot"></span>Ready</div>
      <div class="body">Click "Find article topics" to start.</div>
    </div>

    <div class="log" id="log"></div>

    <div class="section-title" style="margin-top: 32px">
      <span>2. Past results</span>
      <span class="badge" id="runCount">—</span>
    </div>
    <div class="runs" id="runs"><div class="empty">No past runs yet.</div></div>
  </div>

  <div class="col-right">
    <div class="results-meta">
      <span>3. Article ideas</span>
      <span id="filename">— · waiting for first run</span>
    </div>
    <div class="results" id="results">
      <div class="results-empty">No results yet.<br>Set up a search on the left and click "Find article topics".</div>
    </div>
  </div>
</main>

<div class="footer">
  Bienes · Free-tier SEO topic finder · Built 2026
</div>

<script>
const $ = id => document.getElementById(id);

function setTicker(text, klass) {
  const ts = $('tickerStatus');
  ts.innerHTML = `<span><span class="dot ${klass||''}"></span>${text}</span><span id="tickerLast">${$('tickerLast').textContent}</span>`;
}

async function runPipeline(e) {
  e.preventDefault();
  const fd = new FormData($('runform'));
  const body = new URLSearchParams();
  for (const [k, v] of fd) body.append(k, v);
  ['check_freshness', 'diff_last'].forEach(k => {
    if (!fd.get(k)) body.delete(k);
    else body.set(k, '1');
  });
  $('runbtn').disabled = true;
  await fetch('/run', { method: 'POST', body });
  // SSE will pick up the status transition automatically
  return false;
}

// Current client state — populated from SSE snapshot + incremental events.
const state = { running: false, exit_code: null, duration: null, started_at: null };
const logLines = [];

function appendLog(lines) {
  for (const line of lines) logLines.push(line);
  if (logLines.length > 400) logLines.splice(0, logLines.length - 400);
  $('log').textContent = logLines.join('\\n');
  $('log').scrollTop = $('log').scrollHeight;
}

function renderStatus() {
  const st = $('status');
  const s = state;
  if (s.running) {
    const stuckFor = s.started_at ? (Date.now() - new Date(s.started_at).getTime()) / 1000 : 0;
    const lookStuck = stuckFor > 90 && logLines.length === 0;
    st.className = 'statusblock running';
    st.innerHTML = `<div class="head"><span class="dot"></span>Working… (${Math.round(stuckFor)}s elapsed)</div>
      <div class="body">Started ${s.started_at?.split('T')[1] || s.started_at}${lookStuck ? ' — <a href="#" onclick="resetState();return false" style="color:var(--accent);border-bottom:1px solid">looks stuck? click to reset</a>' : ''}</div>`;
    setTicker('WORKING', 'warn');
    $('runbtn').disabled = true;
  } else if (s.exit_code === 0) {
    st.className = 'statusblock done';
    st.innerHTML = `<div class="head"><span class="dot"></span>Done in ${s.duration}s</div><div class="body">Showing the article ideas on the right.</div>`;
    setTicker('LAST RUN: OK', 'on');
    $('tickerLast').textContent = `— · ${s.duration}s · ${s.started_at?.split('T')[1]||''}`;
    $('runbtn').disabled = false;
  } else if (s.exit_code !== null && s.exit_code !== undefined) {
    st.className = 'statusblock error';
    st.innerHTML = `<div class="head"><span class="dot"></span>Failed (error ${s.exit_code})</div><div class="body">After ${s.duration}s — check the log below for what went wrong.</div>`;
    setTicker('FAILED', 'warn');
    $('runbtn').disabled = false;
  } else {
    st.className = 'statusblock idle';
    st.innerHTML = `<div class="head"><span class="dot"></span>Ready</div><div class="body">Click "Find article topics" to start.</div>`;
    setTicker('READY');
    $('runbtn').disabled = false;
  }
}

// Keep a rolling "seconds since started" tick so the UI timer moves even
// when no log lines arrive. Cheap — no network.
setInterval(() => { if (state.running) renderStatus(); }, 1000);

function connectEvents() {
  const es = new EventSource('/events');
  es.addEventListener('snapshot', e => {
    const d = JSON.parse(e.data);
    logLines.length = 0;
    appendLog(d.lines || []);
    Object.assign(state, d);
    renderStatus();
  });
  es.addEventListener('log', e => {
    appendLog(JSON.parse(e.data).lines);
  });
  es.addEventListener('status', e => {
    const wasRunning = state.running;
    const d = JSON.parse(e.data);
    Object.assign(state, d);
    renderStatus();
    // Just transitioned from running → done: reload results + past runs
    if (wasRunning && !state.running && state.exit_code === 0) {
      loadResults();
      loadRuns();
    }
  });
  es.onerror = () => {
    // EventSource auto-reconnects; just rerender so stale "running" doesn't linger
    setTimeout(renderStatus, 1500);
  };
}

async function loadResults(filename) {
  const url = filename ? `/shortlist/${encodeURIComponent(filename)}` : '/results';
  const r = await fetch(url);
  if (!r.ok) {
    $('results').innerHTML = '<div class="results-empty">No edition filed.<br>Run the press to compile.</div>';
    $('filename').textContent = '— · pending issue';
    return;
  }
  const j = await r.json();
  $('filename').textContent = j.filename ? `· ${j.filename}` : '— · pending issue';
  $('results').innerHTML = j.html;
}

async function loadRuns() {
  const r = await fetch('/shortlists');
  const j = await r.json();
  $('runCount').textContent = j.files.length ? `${j.files.length} on file` : '—';
  $('runs').innerHTML = j.files.length
    ? j.files.map(f => `<div class="run" onclick="loadResults('${f.name}')">
         <span>${f.name.replace('batch_shortlist_','#').replace('.md','')}</span>
         <span class="when">${f.when}</span>
       </div>`).join('')
    : '<div class="empty">No archived editions yet.</div>';
}

async function resetState() {
  await fetch('/reset', { method: 'POST' });
  // SSE will push the reset status automatically
}

const seedsParam = new URLSearchParams(location.search).get('seeds');
if (seedsParam) {
  $('seedsBox').value = decodeURIComponent(seedsParam);
  history.replaceState({}, '', '/');
}

loadResults();
loadRuns();
connectEvents();  // live log + status via Server-Sent Events
</script>
</body></html>
"""


DISCOVER_PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Bienes · Suggest topics</title>
<style>""" + SHARED_CSS + """
.help { font: 400 11px/1.4 var(--mono); color: var(--taupe); margin-top: 4px;
        letter-spacing: 0.02em; text-transform: none; }
.surveyhead { padding: 22px 24px; border-bottom: 1px solid var(--rule); position: relative; z-index: 2; }
.surveyhead .lede {
  font: 600 14px/1.45 var(--mono); letter-spacing: 0.04em;
  color: var(--ink-soft); max-width: 64ch;
}
.surveyhead .lede strong { color: var(--accent); }

.survey-grid {
  position: relative; z-index: 2;
  display: grid; grid-template-columns: 360px 1fr; gap: 0;
  border-bottom: 1px solid var(--rule); min-height: 70vh;
}
.survey-grid .col-left { padding: 28px 24px; border-right: 1px solid var(--rule); }
.survey-grid .col-right { padding: 28px 32px; min-width: 0; }

.cand-toolbar {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 14px;
  padding-bottom: 10px;
  border-bottom: 2px solid var(--ink);
}
.cand-toolbar .title {
  font: 700 11px/1 var(--mono); letter-spacing: 0.2em;
  text-transform: uppercase; color: var(--accent);
}
.cand-toolbar .actions { display: flex; gap: 8px; }
.cand-toolbar .actions button {
  padding: 7px 13px; font-size: 9.5px; letter-spacing: 0.14em;
}

.cand-empty {
  font-family: var(--fr); font-style: italic; font-size: 22px;
  color: var(--taupe); text-align: center; padding: 80px 0;
}

.cand-list { font: 500 13px/1.4 var(--mono); }
.cand-row {
  display: grid;
  grid-template-columns: 30px 60px 1fr auto;
  align-items: baseline;
  gap: 14px;
  padding: 11px 0;
  border-bottom: 1px dotted var(--ink-soft);
  cursor: pointer;
  transition: background 0.1s;
}
.cand-row:hover { background: var(--paper-light); }
.cand-row .check {
  width: 16px; height: 16px;
  border: 1.5px solid var(--ink); background: transparent;
  position: relative;
  flex-shrink: 0;
  margin-top: 1px;
}
.cand-row.checked .check { background: var(--ink); }
.cand-row.checked .check::after {
  content: '✓'; position: absolute; inset: 0;
  display: flex; align-items: center; justify-content: center;
  color: var(--paper); font-size: 12px; line-height: 1;
}
.cand-row .scoremark {
  font: 800 26px/1 var(--fr); letter-spacing: -0.04em;
  color: var(--gold); text-align: right; font-feature-settings: 'tnum';
}
.cand-row .scoremark.s2 { color: var(--accent); }
.cand-row .scoremark.s3plus { color: var(--accent); }
.cand-row .phrase {
  font: 500 14px/1.3 var(--mono);
  letter-spacing: -0.005em;
  color: var(--ink);
  word-break: break-word;
}
.cand-row .sources {
  text-align: right;
  font: 500 9.5px/1.3 var(--mono);
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--taupe);
}
.cand-row .sources .src {
  display: inline-block;
  border: 1px solid var(--taupe);
  padding: 1px 5px;
  margin-left: 4px;
}
.cand-row .sources .src.yours { border-color: var(--accent); color: var(--accent); }

.legend {
  margin-top: 24px;
  padding: 14px 16px;
  border: 1px dashed var(--ink-soft);
  font: 500 10.5px/1.55 var(--mono);
  letter-spacing: 0.04em;
  color: var(--ink-soft);
  background: var(--paper-light);
}
.legend strong { color: var(--accent); text-transform: uppercase; letter-spacing: 0.16em; font-size: 10px; }

.btn-primary-large {
  font: 700 12px/1 var(--mono); letter-spacing: 0.18em;
  padding: 14px 26px;
}
</style>
</head><body>

<div class="ticker">
  <div class="left">
    <span><span class="dot on"></span>Bienes</span>
    <span>Suggest topics</span>
  </div>
  <div class="right">
    <a href="/" style="border:0;letter-spacing:0.16em;font-size:10.5px;text-transform:uppercase;color:var(--ink-soft)">‹‹ back to main</a>
  </div>
</div>

<div class="masthead">
  <h1 class="wordmark">Bienes<span class="punct">.</span></h1>
  <div class="dek">
    <div class="label">— Auto-suggest topics —</div>
    Find topic ideas by mining your<br>
    site + competitors + Spanish templates.
  </div>
  <div class="issue">
    HabitaOne / Caracas<br>
    Built 2026
  </div>
</div>

<div class="surveyhead">
  <div class="lede">
    <strong>Don't know what topics to research?</strong> This page suggests them.
    It scans your website's URLs (proven topics for you), your competitors' websites
    (what they cover that you don't), and a Spanish real-estate template
    (common verb + noun + city combos). Pick the ones that fit, then send them
    to the main page.
  </div>
</div>

<main class="survey-grid">
  <div class="col-left">
    <div class="section-title">
      <span>1. What to mine</span>
      <span class="badge">setup</span>
    </div>

    <form id="discform" onsubmit="return discover(event)">
      <div class="field">
        <label class="lbl">Your website</label>
        <input name="domain" placeholder="habitaone.com" value="habitaone.com">
        <div class="help">We'll scan this site's sitemap.xml for topics you already cover.</div>
      </div>

      <div class="field">
        <label class="lbl">Industry / niche keywords</label>
        <input name="niche" placeholder="venezuela real estate" value="venezuela real estate">
        <div class="help">Used to generate template-based topic ideas (e.g. "comprar casa caracas").</div>
      </div>

      <div class="field">
        <label class="lbl">Competitor websites</label>
        <textarea name="competitors">""" + DEFAULT_COMPETITORS + """</textarea>
        <div class="help">Comma-separated domains. We scan each for topic ideas they're targeting.</div>
      </div>

      <div class="field">
        <label class="lbl">Maximum suggestions</label>
        <input name="limit" type="number" value="25" min="5" max="100">
        <div class="help">How many topic ideas to return (sorted best-first).</div>
      </div>

      <button id="discBtn" class="primary btn-primary-large" type="submit" style="margin-top: 14px">
        ▶ Find topic ideas
      </button>
    </form>

    <div class="legend">
      <strong>How to read the results</strong><br><br>
      <strong style="color:var(--accent)">Score</strong> = how strong the signal is. Higher = more sources agree this is a real topic.<br>
      Score 1 = appears in only one source · Score 2-3 = multi-source consensus.<br><br>
      <strong style="color:var(--accent)">Source tags</strong>: <code style="background:transparent;border:0;color:var(--accent)">yours</code> = from your sitemap (strongest signal) ·
      <code style="background:transparent;border:0">comp:*</code> = from a competitor's sitemap ·
      <code style="background:transparent;border:0">template</code> = generated from your niche keywords.
    </div>
  </div>

  <div class="col-right">
    <div class="cand-toolbar">
      <span class="title" id="candTitle">2. Suggestions · click "Find topic ideas" to start</span>
      <div class="actions">
        <button class="secondary" type="button" onclick="selectAll(true)">Select all</button>
        <button class="secondary" type="button" onclick="selectAll(false)">Clear</button>
        <button class="primary" type="button" onclick="useSelected()">› Use selected</button>
      </div>
    </div>
    <div id="candidates" class="cand-empty">
      No suggestions yet.<br>
      <span style="font-size:14px;color:var(--ink-soft);font-family:var(--mono);font-style:normal;letter-spacing:0.08em;">Fill the form on the left, then click the button.</span>
    </div>
  </div>
</main>

<div class="footer">
  Bienes · Topic suggester · Built 2026
</div>

<script>
const $ = id => document.getElementById(id);

async function discover(e) {
  e.preventDefault();
  $('discBtn').disabled = true;
  $('discBtn').textContent = '⋯ working (about 10-30 seconds)';
  $('candidates').innerHTML = '<div class="cand-empty">Scanning sitemaps in parallel… ~10-30 seconds</div>';
  $('candTitle').textContent = '2. Suggestions · working…';
  const fd = new FormData($('discform'));
  const body = new URLSearchParams();
  for (const [k, v] of fd) body.append(k, v);
  try {
    const r = await fetch('/discover/run', { method: 'POST', body });
    const j = await r.json();
    renderCandidates(j.candidates);
    $('candTitle').textContent = `2. Suggestions · ${j.candidates.length} found`;
  } catch (err) {
    $('candidates').innerHTML = '<div class="cand-empty" style="color:var(--accent)">Failed: ' + err.message + '</div>';
    $('candTitle').textContent = '2. Suggestions · error';
  } finally {
    $('discBtn').disabled = false;
    $('discBtn').textContent = '▶ Find topic ideas';
  }
  return false;
}

function renderCandidates(items) {
  if (!items || !items.length) {
    $('candidates').innerHTML = '<div class="cand-empty">No candidates surfaced. Try different inputs.</div>';
    return;
  }
  $('candidates').innerHTML = '<div class="cand-list">' + items.map((c, i) => {
    const checked = c.score >= 2;
    const klass = c.score >= 3 ? 's3plus' : (c.score >= 2 ? 's2' : '');
    const srcs = c.sources.slice(0, 3).map(s => {
      const cls = s === 'yours' ? 'src yours' : 'src';
      const label = s === 'yours' ? 'YOURS' : (s.startsWith('comp:') ? s.slice(5).replace('.com','').replace('.ve','').slice(0,12) : s.toUpperCase());
      return `<span class="${cls}">${label}</span>`;
    }).join('');
    const more = c.sources.length > 3 ? `<span class="src">+${c.sources.length-3}</span>` : '';
    return `<div class="cand-row ${checked?'checked':''}" onclick="toggleRow(this, ${i})" data-phrase="${c.phrase.replace(/"/g, '&quot;')}">
      <div class="check"></div>
      <div class="scoremark ${klass}">${String(c.score).padStart(2,'0')}</div>
      <div class="phrase">${c.phrase}</div>
      <div class="sources">${srcs}${more}</div>
    </div>`;
  }).join('') + '</div>';
}

function toggleRow(el, _i) {
  el.classList.toggle('checked');
}
function selectAll(check) {
  document.querySelectorAll('.cand-row').forEach(r => {
    r.classList.toggle('checked', check);
  });
}
function useSelected() {
  const seeds = Array.from(document.querySelectorAll('.cand-row.checked'))
    .map(r => r.dataset.phrase);
  if (!seeds.length) {
    alert('Select at least one topic first.');
    return;
  }
  location.href = '/?seeds=' + encodeURIComponent(seeds.join('\\n'));
}
</script>
</body></html>
"""


@app.route("/")
def index():
    return PAGE


@app.route("/discover")
def discover_page():
    return DISCOVER_PAGE


@app.route("/discover/run", methods=["POST"])
def discover_run():
    domain = request.form.get("domain", "").strip()
    niche = request.form.get("niche", "").strip()
    comps_raw = request.form.get("competitors", "").strip()
    competitors = [c.strip() for c in comps_raw.split(",") if c.strip()]
    try:
        limit = int(request.form.get("limit", "25"))
    except ValueError:
        limit = 25
    candidates = discover_seeds(
        domain=domain or None,
        niche=niche or None,
        competitors=competitors or None,
        limit=limit,
    )
    return jsonify(candidates=candidates)


@app.route("/run", methods=["POST"])
def run():
    if RUN_STATE["running"]:
        return jsonify(error="already running"), 409

    seeds_raw = request.form.get("seeds", "").strip()
    seeds = [s.strip() for s in seeds_raw.splitlines() if s.strip()]
    if not seeds:
        return jsonify(error="no seeds"), 400

    cmd = [
        PYTHON, str(BATCH),
        "--seeds", ",".join(seeds),
        "--subreddits", request.form.get("subreddits", "").strip(),
        "--locales", request.form.get("locales", "").strip(),
        "--check-serp", request.form.get("check_serp", "0"),
        "--shortlist", request.form.get("shortlist", "12"),
        "--recurse", request.form.get("recurse", "0"),
        "--workers", "4",
        "--rate-limit", "4",
    ]
    if request.form.get("check_freshness"):
        cmd.append("--check-freshness")
    if request.form.get("diff_last"):
        cmd.append("--diff-last")

    threading.Thread(target=_stream_pipeline, args=(cmd,), daemon=True).start()
    return jsonify(started=True), 202


@app.route("/events")
def events():
    """Server-Sent Events: push log lines and status transitions as they happen.

    Replaces the 2s /status polling with live push. Each client keeps its own
    cursor into stdout_lines so reconnects pick up where they left off.
    Heartbeat comment every 15s keeps the CF tunnel connection warm.
    """
    def stream():
        cursor = 0
        last_running = None
        last_exit = None
        heartbeat_at = 0

        # Prime with current snapshot so new clients catch up instantly
        with _LOCK:
            snapshot = {
                "type": "snapshot",
                "running": RUN_STATE["running"],
                "exit_code": RUN_STATE["exit_code"],
                "duration": RUN_STATE["duration"],
                "started_at": RUN_STATE["started_at"],
                "lines": list(RUN_STATE["stdout_lines"]),
            }
            cursor = len(RUN_STATE["stdout_lines"])
            last_running = RUN_STATE["running"]
            last_exit = RUN_STATE["exit_code"]
        yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"

        while True:
            with _LOCK:
                new_lines = RUN_STATE["stdout_lines"][cursor:]
                cursor = len(RUN_STATE["stdout_lines"])
                running = RUN_STATE["running"]
                exit_code = RUN_STATE["exit_code"]
                duration = RUN_STATE["duration"]
                started_at = RUN_STATE["started_at"]

            if new_lines:
                yield f"event: log\ndata: {json.dumps({'lines': new_lines})}\n\n"

            if running != last_running or exit_code != last_exit:
                yield (
                    "event: status\n"
                    f"data: {json.dumps({'running': running, 'exit_code': exit_code, 'duration': duration, 'started_at': started_at})}\n\n"
                )
                last_running, last_exit = running, exit_code

            # Periodic heartbeat so intermediaries don't close idle connections
            heartbeat_at += 1
            if heartbeat_at >= 30:  # ~15s at 0.5s loop
                yield ": ping\n\n"
                heartbeat_at = 0

            time.sleep(0.5)

    return Response(
        stream(), mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # tell Caddy/nginx not to buffer
        },
    )


@app.route("/reset", methods=["POST"])
def reset():
    """Unstick a hung run — safe to call anytime. Only resets our in-memory
    state tracking; doesn't kill any actual subprocess (Coolify handles that
    if you redeploy). Useful after network blips or crashes."""
    with _LOCK:
        stuck_for = None
        if RUN_STATE["running"] and RUN_STATE["started_at"]:
            try:
                dt = datetime.fromisoformat(RUN_STATE["started_at"])
                stuck_for = (datetime.now() - dt).total_seconds()
            except Exception:
                pass
        RUN_STATE.update(running=False, exit_code=None, duration=None,
                         stdout_lines=["(state reset)"])
    return jsonify(ok=True, was_stuck_seconds=stuck_for)


@app.route("/status")
def status():
    with _LOCK:
        return jsonify({
            "running": RUN_STATE["running"],
            "started_at": RUN_STATE["started_at"],
            "stdout_lines": RUN_STATE["stdout_lines"],
            "exit_code": RUN_STATE["exit_code"],
            "duration": RUN_STATE["duration"],
        })


@app.route("/results")
def results():
    p = _latest_shortlist()
    if not p:
        return jsonify(error="none"), 404
    return jsonify(filename=p.name, html=_render_md(p.read_text(encoding="utf-8")))


@app.route("/shortlists")
def shortlists():
    files = []
    for p in _list_shortlists():
        ts = datetime.fromtimestamp(p.stat().st_mtime)
        files.append({"name": p.name, "when": ts.strftime("%Y-%m-%d %H:%M")})
    return jsonify(files=files)


@app.route("/shortlist/<path:filename>")
def shortlist_named(filename):
    # Whitelist: must be in output dir and match pattern
    if not re.match(r"^batch_shortlist_\d{8}_\d{6}\.md$", filename):
        return jsonify(error="invalid"), 400
    p = OUTPUT / filename
    if not p.exists():
        return jsonify(error="not found"), 404
    return jsonify(filename=p.name, html=_render_md(p.read_text(encoding="utf-8")))


if __name__ == "__main__":
    port = int(os.environ.get("GUI_PORT", "5055"))
    print(f"→ http://localhost:{port}")
    app.run(host="127.0.0.1", port=port, debug=False)

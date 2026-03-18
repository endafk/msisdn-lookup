#!/usr/bin/env python3
import argparse
import json
import mmap
import struct
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).parent))
from msisdn_lookup import (
    DEFAULT_DB,
    HASH_SIZE,
    RECORD_SIZE,
    SUFFIX_DIGITS,
    _binary_search,
)

DEFAULT_PORT = 8765

_db_path: Path = DEFAULT_DB


def lookup_hash(hash_hex: str) -> str | None:
    hash_hex = hash_hex.strip().lower()
    if len(hash_hex) != 64:
        return None
    try:
        target = bytes.fromhex(hash_hex)
    except ValueError:
        return None

    if not _db_path.exists():
        raise FileNotFoundError(f"Database not found: {_db_path}")

    db_size = _db_path.stat().st_size
    n_records = db_size // RECORD_SIZE

    with open(_db_path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            suffix = _binary_search(mm, target, n_records)

    if suffix is None:
        return None
    return f"7{suffix:0{SUFFIX_DIGITS}d}"


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MSISDN Lookup</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #0f1117;
    color: #e2e8f0;
    font-family: 'Inter', system-ui, sans-serif;
    padding: 1rem;
  }

  .card {
    width: 100%;
    max-width: 540px;
    background: #1a1d27;
    border: 1px solid #2d3147;
    border-radius: 16px;
    padding: 2.5rem 2rem;
    box-shadow: 0 24px 64px rgba(0,0,0,.5);
  }

  h1 {
    font-size: 1.25rem;
    font-weight: 600;
    letter-spacing: -0.02em;
    color: #f1f5f9;
    margin-bottom: 0.35rem;
  }

  .subtitle {
    font-size: 0.82rem;
    color: #64748b;
    margin-bottom: 2rem;
  }

  label {
    display: block;
    font-size: 0.78rem;
    font-weight: 500;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.5rem;
  }

  textarea {
    width: 100%;
    height: 88px;
    background: #0f1117;
    border: 1px solid #2d3147;
    border-radius: 10px;
    color: #e2e8f0;
    font-family: 'Fira Code', 'Cascadia Code', monospace;
    font-size: 0.82rem;
    padding: 0.75rem 1rem;
    resize: none;
    outline: none;
    transition: border-color .15s;
    line-height: 1.6;
  }
  textarea:focus { border-color: #6366f1; }
  textarea::placeholder { color: #334155; }

  .input-wrapper {
    position: relative;
  }

  #btn-paste {
    position: absolute;
    top: 0.5rem;
    right: 0.5rem;
    display: flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.3rem 0.6rem;
    background: #1e2130;
    border: 1px solid #2d3147;
    border-radius: 6px;
    color: #64748b;
    font-size: 0.72rem;
    font-weight: 600;
    cursor: pointer;
    transition: color .15s, border-color .15s;
    flex: none;
  }
  #btn-paste:hover { color: #a5b4fc; border-color: #6366f1; }
  #btn-paste:active { transform: scale(.95); }

  .actions {
    display: flex;
    gap: 0.6rem;
    margin-top: 1rem;
  }

  button {
    flex: 1;
    padding: 0.65rem 1rem;
    border: none;
    border-radius: 8px;
    font-size: 0.875rem;
    font-weight: 600;
    cursor: pointer;
    transition: opacity .15s, transform .1s;
  }
  button:active { transform: scale(.97); }

  #btn-lookup {
    background: #6366f1;
    color: #fff;
  }
  #btn-lookup:hover { opacity: .88; }

  #btn-clear {
    background: #1e2130;
    color: #64748b;
    border: 1px solid #2d3147;
    flex: 0 0 auto;
    padding-left: 1.2rem;
    padding-right: 1.2rem;
  }
  #btn-clear:hover { color: #94a3b8; }

  #result-box {
    margin-top: 1.75rem;
    display: none;
  }

  .result-label {
    font-size: 0.78rem;
    font-weight: 500;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.5rem;
  }

  #result-display {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    background: #0f1117;
    border: 1px solid #2d3147;
    border-radius: 10px;
    padding: 0.85rem 1rem;
    cursor: pointer;
    transition: border-color .15s, background .15s;
  }
  #result-display:hover { border-color: #6366f1; background: #13161f; }

  #phone-number {
    flex: 1;
    font-family: 'Fira Code', 'Cascadia Code', monospace;
    font-size: 1.4rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    color: #a5b4fc;
  }

  #copy-icon {
    flex-shrink: 0;
    color: #475569;
    transition: color .15s;
  }
  #result-display:hover #copy-icon { color: #6366f1; }

  #copy-feedback {
    font-size: 0.75rem;
    color: #22c55e;
    margin-top: 0.45rem;
    min-height: 1.1em;
    transition: opacity .3s;
  }

  #error-box {
    margin-top: 1.75rem;
    display: none;
    background: #1f1318;
    border: 1px solid #4c1d26;
    border-radius: 10px;
    padding: 0.85rem 1rem;
    color: #f87171;
    font-size: 0.875rem;
  }

  .spinner {
    width: 16px; height: 16px;
    border: 2px solid rgba(255,255,255,.2);
    border-top-color: #fff;
    border-radius: 50%;
    animation: spin .6s linear infinite;
    display: inline-block;
    margin-right: 6px;
    vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div class="card">
  <h1>MSISDN Lookup</h1>
  <p class="subtitle">Reverse an MPesa Daraja SHA-256 hash to a phone number</p>

  <label for="hash-input">SHA-256 hash</label>
  <div class="input-wrapper">
    <textarea
      id="hash-input"
      spellcheck="false"
      autocomplete="off"
      placeholder="Paste 64-char hex hash here…"
    ></textarea>
    <button id="btn-paste" type="button" title="Paste from clipboard">
      <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12"
           viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"></path>
        <rect x="8" y="2" width="8" height="4" rx="1" ry="1"></rect>
      </svg>
      Paste
    </button>
  </div>

  <div class="actions">
    <button id="btn-lookup">Look up</button>
    <button id="btn-clear">Clear</button>
  </div>

  <div id="result-box">
    <div class="result-label">Phone number</div>
    <div id="result-display" title="Click to copy">
      <span id="phone-number"></span>
      <svg id="copy-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18"
           viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
      </svg>
    </div>
    <div id="copy-feedback"></div>
  </div>

  <div id="error-box"></div>
</div>

<script>
const input       = document.getElementById('hash-input');
const btnLookup   = document.getElementById('btn-lookup');
const btnClear    = document.getElementById('btn-clear');
const btnPaste    = document.getElementById('btn-paste');
const resultBox   = document.getElementById('result-box');
const phoneEl     = document.getElementById('phone-number');
const resultDisp  = document.getElementById('result-display');
const copyFb      = document.getElementById('copy-feedback');
const errorBox    = document.getElementById('error-box');

function setResult(phone) {
  phoneEl.textContent = phone;
  resultBox.style.display = 'block';
  errorBox.style.display  = 'none';
  copyFb.textContent = '';
}

function setError(msg) {
  errorBox.textContent = msg;
  errorBox.style.display = 'block';
  resultBox.style.display = 'none';
}

function clearAll() {
  input.value = '';
  resultBox.style.display = 'none';
  errorBox.style.display  = 'none';
  copyFb.textContent = '';
  input.focus();
}

async function doLookup() {
  const hash = input.value.trim();
  if (!hash) return;

  if (hash.length !== 64 || !/^[0-9a-fA-F]+$/.test(hash)) {
    setError('Invalid hash — expected a 64-character hex string.');
    return;
  }

  btnLookup.innerHTML = '<span class="spinner"></span>Looking up…';
  btnLookup.disabled = true;

  try {
    const res  = await fetch('/lookup?hash=' + encodeURIComponent(hash));
    const data = await res.json();
    if (data.phone) {
      setResult(data.phone);
    } else {
      setError(data.error || 'Hash not found in database.');
    }
  } catch (e) {
    setError('Request failed — is the server running?');
  } finally {
    btnLookup.innerHTML = 'Look up';
    btnLookup.disabled  = false;
  }
}

resultDisp.addEventListener('click', async () => {
  const phone = phoneEl.textContent;
  if (!phone) return;
  try {
    await navigator.clipboard.writeText(phone);
    copyFb.textContent = 'Copied!';
    setTimeout(() => { copyFb.textContent = ''; }, 2000);
  } catch {
    copyFb.textContent = 'Copy failed — try manually.';
  }
});

btnLookup.addEventListener('click', doLookup);
btnClear.addEventListener('click', clearAll);

btnPaste.addEventListener('click', async () => {
  try {
    const text = await navigator.clipboard.readText();
    input.value = text.trim();
    if (input.value.length === 64) doLookup();
    else input.focus();
  } catch {
    input.focus();
  }
});

input.addEventListener('paste', () => {
  setTimeout(() => {
    if (input.value.trim().length === 64) doLookup();
  }, 50);
});

input.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doLookup(); }
});
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/":
            self._send(200, "text/html; charset=utf-8", HTML.encode())

        elif parsed.path == "/lookup":
            params = parse_qs(parsed.query)
            hash_hex = (params.get("hash") or [""])[0]
            try:
                phone = lookup_hash(hash_hex)
                if phone:
                    body = json.dumps({"phone": phone})
                else:
                    body = json.dumps({"error": "Hash not found in database."})
            except FileNotFoundError as e:
                body = json.dumps({"error": str(e)})
            self._send(200, "application/json", body.encode())

        else:
            self._send(404, "text/plain", b"Not found")


def main() -> None:
    parser = argparse.ArgumentParser(description="MSISDN Lookup web frontend")
    parser.add_argument("--db", default=str(DEFAULT_DB), metavar="PATH",
                        help=f"Database path (default: {DEFAULT_DB})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"Port to listen on (default: {DEFAULT_PORT})")
    args = parser.parse_args()

    global _db_path
    _db_path = Path(args.db)

    if not _db_path.exists():
        print(f"")
        print(f"  WARNING: database not found at {_db_path}")
        print(f"  Build it first with:")
        print(f"    python msisdn_lookup.py build")
        print(f"  Lookups will return an error until the database is present.")
        print(f"")

    server = HTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://localhost:{args.port}"
    print(f"Listening on {url}")
    print("Press Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()

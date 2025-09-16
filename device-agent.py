#!/usr/bin/env python3
"""
Python device log agent
- Sources: journald (-o json -f) or tail -F file(s)
- Transport: WebSocket (ws:// or wss://), 1 JSON record per message
- Offline-friendly: spools to disk and drains on reconnect
- Config: env vars or CLI flags (see bottom)

Test mode: --demo (generates synthetic logs)
"""

import asyncio, json, os, sys, time, ssl, signal, pathlib, random, subprocess, shlex
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

# ---------- Config helpers ----------
def env(key, default=None, cast=str):
    v = os.environ.get(key, None)
    if v is None: return default
    try: return cast(v)
    except: return v

SERVER     = env("SERVER_URL", "ws://127.0.0.1:4000/ingest")
DEVICE_ID  = env("DEVICE_ID", (pathlib.Path("/etc/machine-id").read_text().strip() if pathlib.Path("/etc/machine-id").exists() else os.uname().nodename))
INPUT      = env("INPUT", "journal")               # 'journal' | 'files' | 'demo'
FILES      = env("FILES", "")                      # comma-separated, glob patterns allowed
SPOOL_DIR  = pathlib.Path(env("SPOOL_DIR", "/var/lib/device-agent/spool"))
SPOOL_MAX_FILE = int(env("SPOOL_MAX_FILE", 1024*1024))  # bytes per spool file (~1MB)
PING_SEC   = int(env("PING_SEC", 30))
RECONNECT_MIN = float(env("RECONNECT_MIN", 1.5))
RECONNECT_MAX = float(env("RECONNECT_MAX", 20.0))
VERBOSE    = env("VERBOSE", "0") == "1"

# WebSocket library
try:
    import websockets
except Exception as e:
    print("ERROR: This agent needs the 'websockets' package.\n"
          "Install with: pip3 install websockets", file=sys.stderr)
    sys.exit(2)

SPOOL_DIR.mkdir(parents=True, exist_ok=True)

def build_server_url(base, device_id):
    """Append device_id query if not present."""
    u = urlparse(base)
    qs = dict(parse_qsl(u.query))
    qs.setdefault("device_id", device_id)
    return urlunparse((u.scheme, u.netloc, u.path or "/ingest", u.params, urlencode(qs), u.fragment))

SERVER_URL = build_server_url(SERVER, DEVICE_ID)

# ---------- Spool (durable queue) ----------
class Spool:
    def __init__(self, dirpath: pathlib.Path, max_file: int = 1_000_000):
        self.dir = dirpath
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_file = max_file
        self.current = self.dir / "current.jsonl"

    def _roll(self):
        if self.current.exists() and self.current.stat().st_size >= self.max_file:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            self.current.rename(self.dir / f"spool-{ts}.jsonl")

    def put(self, line: str):
        self._roll()
        with self.current.open("a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")

    def files(self):
        files = [p for p in self.dir.glob("*.jsonl")]
        # ensure 'current' is last so older files drain first
        files.sort(key=lambda p: (p.name.startswith("current"), p.stat().st_mtime))
        return files

    def drain_iter(self):
        """Yield (path, iterator over lines). Caller deletes file after success."""
        for f in self.files():
            try:
                with f.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.rstrip("\n")
                        if line:
                            yield (f, line)
            except Exception as e:
                print(f"[spool] read error {f}: {e}", file=sys.stderr)

spool = Spool(SPOOL_DIR, SPOOL_MAX_FILE)

# ---------- Log sources ----------
async def read_journal(queue: asyncio.Queue):
    """
    Follow journald as JSON lines (robust for BusyBox/Yocto):
    - use stdbuf if available to force line-buffered stdout
    - read with readline() to avoid iterator buffering edge cases
    Env overrides:
      JOURNAL_N: initial backfill count (default 0)
      JOURNAL_UNIT: optional systemd unit filter (e.g. "sshd.service")
      JOURNAL_SINCE: optional since string (e.g. "5 minutes ago")
    """
    import shutil
    n = os.environ.get("JOURNAL_N", "0")
    unit = os.environ.get("JOURNAL_UNIT")
    since = os.environ.get("JOURNAL_SINCE")

    base = ["journalctl", "-o", "json", "--no-pager"]
    # single process that both backfills and follows
    cmd = base + ["-f", "-n", str(n)]
    if unit:
        cmd += ["-u", unit]
    if since:
        cmd += ["--since", since]

    prefix = ["stdbuf", "-oL", "-eL"] if shutil.which("stdbuf") else []
    full_cmd = prefix + cmd

    if VERBOSE:
        print("[src] exec:", " ".join(full_cmd))

    proc = await asyncio.create_subprocess_exec(
        *full_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # NOTE: leave text=False; we decode manually to control errors/newlines
    )

    async def _stderr():
        async for bline in proc.stderr:
            if VERBOSE:
                try:
                    print("[journalctl]", bline.decode("utf-8", "replace").rstrip(), file=sys.stderr)
                except Exception:
                    pass
    asyncio.create_task(_stderr())

    # Read line-by-line (robust on non-tty pipes)
    while True:
        bline = await proc.stdout.readline()
        if not bline:
            # process ended or temporary hiccup; brief nap to avoid spin
            await asyncio.sleep(0.05)
            if proc.returncode is not None:
                if VERBOSE:
                    print(f"[src] journalctl exited rc={proc.returncode}")
                break
            continue

        line = bline.decode("utf-8", "replace").strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            obj = {"MESSAGE": line}

        obj.setdefault("device_id", DEVICE_ID)
        obj.setdefault("TS", datetime.now(timezone.utc).isoformat())

        if VERBOSE:
            # keep it short in logs
            preview = obj.get("MESSAGE") if isinstance(obj, dict) else str(obj)
            if preview is not None:
                preview = str(preview)
            print("[src] journal -> queue:", (preview[:120] + "…") if preview and len(preview) > 120 else preview)

        await queue.put(json.dumps(obj, separators=(",", ":")))

async def read_files(queue: asyncio.Queue, files_csv: str):
    """
    Tail plain files via 'tail -n0 -f' (use -f for BusyBox compatibility).
    Globs are expanded by the shell.
    """
    files_csv = (files_csv or "").strip()
    if not files_csv:
        print("[src] FILES is empty for INPUT=files", file=sys.stderr)
        return

    # Use /bin/sh so globs expand; fall back to -f (BusyBox often lacks -F)
    cmd = f"exec tail -n0 -f {files_csv}"
    if VERBOSE:
        print("[src] exec:", cmd)

    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def _stderr():
        async for bline in proc.stderr:
            if VERBOSE:
                print("[tail]", bline.decode("utf-8", "replace").rstrip(), file=sys.stderr)
    asyncio.create_task(_stderr())

    while True:
        bline = await proc.stdout.readline()
        if not bline:
            await asyncio.sleep(0.05)
            if proc.returncode is not None:
                if VERBOSE:
                    print(f"[src] tail exited rc={proc.returncode}")
                break
            continue

        msg = bline.decode("utf-8", "replace").rstrip("\n")
        if not msg:
            continue
        obj = {"MESSAGE": msg, "device_id": DEVICE_ID, "TS": datetime.now(timezone.utc).isoformat()}
        if VERBOSE:
            print("[src] tail -> queue:", (msg[:120] + "…") if len(msg) > 120 else msg)
        await queue.put(json.dumps(obj, separators=(",", ":")))

async def read_demo(queue: asyncio.Queue):
    units = ["app.service","net.service","update.service","sensor.service"]
    levels = ["DEBUG","INFO","WARN","ERROR"]
    i = 0
    while True:
        i += 1
        obj = {
            "device_id": DEVICE_ID,
            "_SYSTEMD_UNIT": units[i % len(units)],
            "PRIO": levels[(i % 17) % len(levels)],
            "MESSAGE": f"demo event (seq={i})",
            "TS": datetime.now(timezone.utc).isoformat()
        }
        await queue.put(json.dumps(obj, separators=(",", ":")))
        await asyncio.sleep(0.2)  # 5 msgs/sec

# ---------- Sender ----------
async def send_loop(queue: asyncio.Queue):
    """
    Maintain a WS connection; drain spool first, then send live queue.
    On failure: append to spool, backoff, and reconnect.
    """
    import websockets
    backoff = RECONNECT_MIN

    while True:
        ssl_ctx = None
        if SERVER_URL.startswith("wss://"):
            ssl_ctx = ssl.create_default_context()
            # For lab/testing with self-signed certs, allow override:
            if env("TLS_INSECURE", "0") == "1":
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

        try:
            if VERBOSE: print(f"[ws] connecting {SERVER_URL}")
            async with websockets.connect(
                SERVER_URL, ping_interval=PING_SEC, ping_timeout=PING_SEC*2,
                max_queue=None, ssl=ssl_ctx
            ) as ws:
                if VERBOSE: print("[ws] connected")

                # 1) drain any spooled messages first
                for f, line in list(spool.drain_iter()):
                    try:
                        await ws.send(line)
                    except Exception as e:
                        if VERBOSE: print(f"[ws] drain failed ({e}); keeping {f.name}")
                        # stop draining; keep file for later
                        raise
                # If we fully drained older files, we can safely remove them now
                # (We remove after each full drain to keep it simple.)
                for f in list(spool.files()):
                    # remove files that are not 'current'
                    if f.name != "current.jsonl":
                        try: f.unlink()
                        except: pass

                # 2) stream live queue
                backoff = RECONNECT_MIN
                while True:
                    line = await queue.get()
                    try:
                        await ws.send(line)
                    except Exception as e:
                        if VERBOSE: print(f"[ws] send failed: {e}; spooling")
                        spool.put(line)
                        raise
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if VERBOSE: print(f"[ws] disconnected: {e}")
            # Backoff with jitter
            delay = backoff + random.random() * 0.5
            backoff = min(RECONNECT_MAX, backoff * 1.7)
            await asyncio.sleep(delay)

# ---------- Main ----------
async def main():
    print(f"[agent] device_id={DEVICE_ID} input={INPUT} url={SERVER_URL}")
    q = asyncio.Queue(maxsize=10_000)

    # Start source
    if INPUT == "journal":
        src = asyncio.create_task(read_journal(q))
    elif INPUT == "files":
        src = asyncio.create_task(read_files(q, FILES))
    elif INPUT == "demo":
        src = asyncio.create_task(read_demo(q))
    else:
        print(f"[agent] unknown INPUT={INPUT}", file=sys.stderr)
        return 2

    # Sender
    sender = asyncio.create_task(send_loop(q))

    # Graceful shutdown: flush in-memory queue to spool
    stopping = asyncio.Event()
    def _stop(*_):
        if not stopping.is_set():
            print("[agent] stopping… flushing queue to spool")
            stopping.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _stop)

    await stopping.wait()
    try:
        src.cancel(); sender.cancel()
    except: pass

    # Drain queue to spool
    while not q.empty():
        try: spool.put(q.get_nowait())
        except: break
    print("[agent] bye")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


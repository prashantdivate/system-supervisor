#!/usr/bin/env python3
"""
Python device log agent
- Sources: journald (-o json -f) or tail -f file(s) or demo generator
- Transport: WebSocket (ws:// or wss://), 1 JSON record per message
- Offline-friendly: spools to disk and drains on reconnect
- Snapshots: periodic "type: snapshot" frames with OS/IP/containers for UI Summary
- Optional control: receive {type:'control',action:'reboot'} from server when AGENT_ALLOW_CONTROL=1
Requires: pip3 install websockets
"""

import asyncio, json, os, sys, time, ssl, signal, pathlib, random, subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

# ---------- Config helpers ----------
def env(key, default=None, cast=str):
    v = os.environ.get(key, None)
    if v is None:
        return default
    try:
        return cast(v)
    except:
        return v

SERVER     = env("SERVER_URL", "ws://127.0.0.1:4000/ingest")
DEVICE_ID  = env("DEVICE_ID", (pathlib.Path("/etc/machine-id").read_text().strip()
                               if pathlib.Path("/etc/machine-id").exists()
                               else os.uname().nodename))
INPUT      = env("INPUT", "journal")               # 'journal' | 'files' | 'demo'
FILES      = env("FILES", "")                      # comma-separated, glob patterns allowed
SPOOL_DIR  = pathlib.Path(env("SPOOL_DIR", "/var/lib/device-agent/spool"))
SPOOL_MAX_FILE = int(env("SPOOL_MAX_FILE", 1024*1024))  # bytes per spool file (~1MB)
PING_SEC   = int(env("PING_SEC", 30))
RECONNECT_MIN = float(env("RECONNECT_MIN", 1.5))
RECONNECT_MAX = float(env("RECONNECT_MAX", 20.0))
VERBOSE    = env("VERBOSE", "0") == "1"

# Control channel
ALLOW_CONTROL = (env("AGENT_ALLOW_CONTROL", "0") == "1")

# Snapshot heartbeat for Summary panel
SNAPSHOT_SEC = int(env("SNAPSHOT_SEC", 10))  # <=0 disables snapshots
RUNTIME_OVERRIDE = env("CONTAINER_RUNTIME", "")  # 'docker' | 'podman' (optional)

# WebSocket library
try:
    import websockets
except Exception:
    print("ERROR: This agent needs the 'websockets' package.\nInstall with: pip3 install websockets", file=sys.stderr)
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
    import shutil
    n = os.environ.get("JOURNAL_N", "0")
    unit = os.environ.get("JOURNAL_UNIT")
    since = os.environ.get("JOURNAL_SINCE")

    base = ["journalctl", "-o", "json", "--no-pager"]
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
    )

    async def _stderr():
        async for bline in proc.stderr:
            if VERBOSE:
                try:
                    print("[journalctl]", bline.decode("utf-8", "replace").rstrip(), file=sys.stderr)
                except Exception:
                    pass
    asyncio.create_task(_stderr())

    while True:
        bline = await proc.stdout.readline()
        if not bline:
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
            preview = obj.get("MESSAGE") if isinstance(obj, dict) else str(obj)
            preview = str(preview) if preview is not None else ""
            print("[src] journal -> queue:", (preview[:120] + "…") if preview and len(preview) > 120 else preview)

        await queue.put(json.dumps(obj, separators=(",", ":")))

async def read_files(queue: asyncio.Queue, files_csv: str):
    files_csv = (files_csv or "").strip()
    if not files_csv:
        print("[src] FILES is empty for INPUT=files", file=sys.stderr)
        return

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

# ---------- Snapshot & system info ----------
def _sh(cmd: str):
    try:
        out = subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL)
        return 0, out.decode("utf-8", "replace").strip()
    except subprocess.CalledProcessError as e:
        return e.returncode, (e.output.decode("utf-8", "replace") if e.output else "").strip()

def detect_runtime():
    if RUNTIME_OVERRIDE:
        return RUNTIME_OVERRIDE
    try:
        import shutil
        if shutil.which("podman"):
            return "podman"
        if shutil.which("docker"):
            return "docker"
    except Exception:
        pass
    return None

def _parse_ps_pipe_lines(out):
    items = []
    for ln in out.splitlines():
        parts = ln.split("|")
        if len(parts) >= 4:
            cid, names, image, status = parts[:4]
            ports = parts[4] if len(parts) > 4 else ""
            items.append({
                "id": cid.strip(),
                "name": (names or "").strip(),
                "image": (image or "").strip(),
                "status": (status or "").strip(),
                "state": "",
                "ports": (ports or "").strip(),
            })
    return items

def list_containers():
    r = detect_runtime()
    if not r:
        if VERBOSE: print("[snap] no container runtime found")
        return []

    if r == "podman":
        rc, out = _sh("podman ps --all --format json")
        if rc == 0 and out:
            try:
                arr = json.loads(out)
                items = []
                for row in arr:
                    items.append({
                        "id": row.get("Id") or row.get("ID"),
                        "name": (row.get("Names") or row.get("Name") or [""])[0] if isinstance(row.get("Names"), list) else (row.get("Names") or row.get("Name")),
                        "image": row.get("Image") or row.get("ImageName"),
                        "status": row.get("Status") or row.get("State"),
                        "state": row.get("State") or "",
                        "ports": row.get("Ports") if isinstance(row.get("Ports"), str) else "",
                    })
                if items: return items
            except Exception as e:
                if VERBOSE: print("[snap] podman json array parse failed:", e)

        rc, out = _sh("podman ps -a --format '{{json .}}'")
        if rc == 0 and out:
            items = []
            for line in out.splitlines():
                line = line.strip()
                if not line: continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                items.append({
                    "id": row.get("ID") or row.get("Id"),
                    "name": row.get("Names") or row.get("Name"),
                    "image": row.get("Image") or row.get("ImageName"),
                    "status": row.get("Status") or row.get("State"),
                    "state": row.get("State") or "",
                    "ports": row.get("Ports") if isinstance(row.get("Ports"), str) else "",
                })
            if items: return items

        rc, out = _sh("podman ps -a --format '{{.ID}}|{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}'")
        if rc == 0 and out:
            return _parse_ps_pipe_lines(out)
        if VERBOSE: print("[snap] podman ps produced no containers")
        return []

    rc, out = _sh("docker ps -a --format '{{json .}}'")
    if rc == 0 and out:
        items = []
        for line in out.splitlines():
            line = line.strip()
            if not line: continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            items.append({
                "id": row.get("ID") or row.get("Id"),
                "name": row.get("Names") or row.get("Name"),
                "image": row.get("Image"),
                "status": row.get("Status"),
                "state": row.get("State") or "",
                "ports": row.get("Ports"),
            })
        if items: return items

    rc, out = _sh("docker ps -a --format '{{.ID}}|{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}'")
    if rc == 0 and out:
        return _parse_ps_pipe_lines(out)
    if VERBOSE: print("[snap] docker ps produced no containers")
    return []

def get_os_info():
    name = version = build = ""
    try:
        with open("/etc/os-release", "r", encoding="utf-8") as f:
            data = {}
            for ln in f:
                if "=" in ln:
                    k, v = ln.split("=", 1)
                    data[k.strip()] = v.strip().strip('"')
            name = data.get("NAME", "")
            version = data.get("VERSION", "") or data.get("VERSION_ID", "")
            build = data.get("BUILD_ID", "") or data.get("IMAGE_ID", "") or data.get("PRETTY_NAME", "")
    except Exception:
        pass
    rc, kernel = _sh("uname -r")
    return {"name": name, "version": version, "kernel": kernel, "build": build}

def get_ips():
    rc, out = _sh("ip -4 -o addr show scope global | awk '{print $2\"|\"$4}'")
    ips = []
    if rc == 0 and out:
        for ln in out.splitlines():
            try:
                iface, cidr = ln.split("|", 1)
                ips.append({"if": iface, "cidr": cidr})
            except Exception:
                pass
    return ips

async def collect_snapshot():
    return {
        "type": "snapshot",
        "device_id": DEVICE_ID,
        "ts": datetime.now(timezone.utc).isoformat(),
        "os": get_os_info(),
        "ips": get_ips(),
        "runtime": detect_runtime(),
        "containers": list_containers(),
        "control": {"enabled": bool(ALLOW_CONTROL)},   # <-- advertise capability
    }

async def snapshot_loop(queue: asyncio.Queue, interval_sec: int):
    if interval_sec <= 0:
        if VERBOSE:
            print("[snap] disabled (SNAPSHOT_SEC<=0)")
        return
    while True:
        snap = await collect_snapshot()
        if VERBOSE:
            cnt = len(snap.get("containers", []))
            print(f"[snap] send: {snap['os'].get('name','')} {snap['os'].get('version','')} | {len(snap.get('ips',[]))} ip(s) | {cnt} container(s)")
        await queue.put(json.dumps(snap, separators=(",", ":")))
        await asyncio.sleep(interval_sec)

# ---------- Control receiver ----------
async def _do_reboot():
    if VERBOSE: print("[ctl] executing reboot")
    try:
        # pick something that's present on most distros
        subprocess.Popen(["sh", "-lc", "systemctl reboot || reboot -f || /sbin/reboot -f >/dev/null 2>&1 &"])
    except Exception as e:
        print("[ctl] reboot spawn failed:", e, file=sys.stderr)

async def control_receiver(ws):
    if not ALLOW_CONTROL:
        return
    print("[agent] control receiver ENABLED (AGENT_ALLOW_CONTROL=1)")
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if not isinstance(msg, dict):
                continue
            if msg.get("type") != "control":
                continue

            action = msg.get("action")
            if VERBOSE: print("[ctl] received:", msg)

            if action == "reboot":
                await _do_reboot()
            # (future) elif action == "container": ...
    except Exception as e:
        if VERBOSE:
            print("[ctl] receiver ended:", e)

# ---------- Sender ----------
async def send_loop(queue: asyncio.Queue):
    import websockets
    backoff = RECONNECT_MIN

    while True:
        ssl_ctx = None
        if SERVER_URL.startswith("wss://"):
            ssl_ctx = ssl.create_default_context()
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

                # tell the server our capabilities (incl. control)
                hello = {"type":"hello","device_id":DEVICE_ID,"control":{"enabled":bool(ALLOW_CONTROL)}}
                try:
                    await ws.send(json.dumps(hello, separators=(",",":")))
                except Exception:
                    pass

                # start control receiver (non-blocking)
                ctrl_task = asyncio.create_task(control_receiver(ws))

                # 1) drain any spooled messages first
                for f, line in list(spool.drain_iter()):
                    try:
                        await ws.send(line)
                    except Exception as e:
                        if VERBOSE: print(f"[ws] drain failed ({e}); keeping {f.name}")
                        raise
                for f in list(spool.files()):
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
            delay = backoff + random.random() * 0.5
            backoff = min(RECONNECT_MAX, backoff * 1.7)
            await asyncio.sleep(delay)

# ---------- Main ----------
async def main():
    print(f"[agent] device_id={DEVICE_ID} input={INPUT} url={SERVER_URL}")
    if ALLOW_CONTROL:
        print("[agent] control receiver ENABLED (AGENT_ALLOW_CONTROL=1)")
    q = asyncio.Queue(maxsize=10_000)

    if INPUT == "journal":
        src = asyncio.create_task(read_journal(q))
    elif INPUT == "files":
        src = asyncio.create_task(read_files(q, FILES))
    elif INPUT == "demo":
        src = asyncio.create_task(read_demo(q))
    else:
        print(f"[agent] unknown INPUT={INPUT}", file=sys.stderr)
        return 2

    snap_task = asyncio.create_task(snapshot_loop(q, SNAPSHOT_SEC))
    sender = asyncio.create_task(send_loop(q))

    stopping = asyncio.Event()
    def _stop(*_):
        if not stopping.is_set():
            print("[agent] stopping… flushing queue to spool")
            stopping.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _stop)
        except Exception:
            pass

    await stopping.wait()
    try:
        src.cancel(); sender.cancel(); snap_task.cancel()
    except:
        pass

    while not q.empty():
        try: spool.put(q.get_nowait())
        except: break
    print("[agent] bye")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


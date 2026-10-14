#!/usr/bin/env python3
"""yadisk-downloader: download public Yandex Disk links with a local web UI.

Pure standard library. Run `python3 yadisk.py` and open http://127.0.0.1:8765
"""
import argparse
import collections
import csv
import hashlib
import io
import json
import os
import platform
import re
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

API = os.environ.get("YADISK_API", "https://cloud-api.yandex.net/v1/disk/public/resources")
HERE = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
LINK_RE = re.compile(r"https?://(?:disk\.yandex\.[a-z]+|disk\.360\.yandex\.[a-z]+|yadi\.sk)/[di]/[\w\-]+")
INVALID = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def safe_name(name):
    name = INVALID.sub("_", name).strip(" .") or "_"
    return name[:200]


def inside(base, target):
    base, target = Path(base).resolve(), Path(target).resolve()
    return base == target or base in target.parents


def classify(exc):
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    if isinstance(exc, (TimeoutError,)) or "timed out" in str(exc).lower():
        return "TIMEOUT"
    if isinstance(exc, (urllib.error.URLError, ConnectionError, OSError)):
        return "NETWORK"
    return str(exc)[:80] or exc.__class__.__name__


class Fail(Exception):
    pass


def api_get(url, params, timeout=20, tries=4):
    q = API + url + "?" + urllib.parse.urlencode(params)
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(q, headers={"User-Agent": "yadisk-downloader"}), timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (400, 401, 403, 404):
                raise
            time.sleep(1.5 * (i + 1))
        except Exception as e:  # network hiccup
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


class Keepawake:
    def __init__(self):
        self.proc = None

    def on(self):
        try:
            if platform.system() == "Darwin" and not self.proc:
                self.proc = subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])
            elif platform.system() == "Windows":
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
        except Exception:
            pass

    def off(self):
        try:
            if self.proc:
                self.proc.terminate()
                self.proc = None
            if platform.system() == "Windows":
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
        except Exception:
            pass


class Engine:
    def __init__(self, data_dir, default_dest, threads=10, mode="parallel", header_timeout=8):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.data_dir / "state.db", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY, url TEXT UNIQUE, name TEXT, dest TEXT,
                status TEXT DEFAULT 'listing', err TEXT);
            CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY, job_id INTEGER, rpath TEXT, size INTEGER,
                md5 TEXT, preview TEXT, target TEXT, status TEXT DEFAULT 'pending', err TEXT, attempts INTEGER DEFAULT 0,
                UNIQUE(job_id, rpath));
            CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
            CREATE INDEX IF NOT EXISTS f_job_status ON files(job_id, status);
        """)
        self.settings = {"threads": threads, "mode": mode, "dest": str(default_dest), "header_timeout": header_timeout}
        for row in self.db.execute("SELECT k, v FROM settings"):
            self.settings[row["k"]] = json.loads(row["v"])
        self.paused = False
        self.running = set()
        self.inflight = collections.Counter()
        self.errors = collections.deque(maxlen=50)
        self.samples = collections.deque(maxlen=30)
        self.bytes_done_session = 0
        self.workers = []
        self.stop = False
        self.auto_retried = False
        self.keep = Keepawake()
        self.db.execute("UPDATE jobs SET status='listing' WHERE status='listing'")
        self.db.commit()

    def one(self, sql, args=()):
        with self.lock:
            return self.db.execute(sql, args).fetchone()

    def all(self, sql, args=()):
        with self.lock:
            return self.db.execute(sql, args).fetchall()

    # ---------- settings ----------
    def set_settings(self, **kw):
        with self.lock:
            for k, v in kw.items():
                if k in ("threads", "header_timeout"):
                    v = max(1, min(int(v), 64))
                if k == "mode" and v not in ("parallel", "sequential"):
                    continue
                self.settings[k] = v
                self.db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, json.dumps(v)))
            self.db.commit()
        self.ensure_workers()

    # ---------- jobs ----------
    def add_links(self, text, dest=None):
        urls = LINK_RE.findall(text)
        added = []
        for u in dict.fromkeys(urls):
            with self.lock:
                cur = self.db.execute("INSERT OR IGNORE INTO jobs(url, dest) VALUES(?,?)", (u, dest or self.settings["dest"]))
                self.db.commit()
                if cur.rowcount:
                    jid = cur.lastrowid
                    threading.Thread(target=self.list_job, args=(jid,), daemon=True).start()
                    added.append(u)
        self.ensure_workers()
        return added

    def resume_unfinished(self):
        for r in self.all("SELECT id FROM jobs WHERE status='listing'"):
            threading.Thread(target=self.list_job, args=(r["id"],), daemon=True).start()
        self.ensure_workers()

    def list_job(self, jid):
        job = self.one("SELECT * FROM jobs WHERE id=?", (jid,))
        try:
            root = api_get("", {"public_key": job["url"], "limit": 100, "preview_size": "1280x"})
            name = safe_name(root.get("name") or f"job{jid}")
            if self.one("SELECT 1 FROM jobs WHERE name=? AND dest=? AND id<>?", (name, job["dest"], jid)):
                name = f"{name} ({jid})"
            base = Path(job["dest"]) / name
            with self.lock:
                self.db.execute("UPDATE jobs SET name=? WHERE id=?", (name, jid))
                self.db.commit()
            if root.get("type") != "dir":
                self.add_file(jid, "/", root, base.parent / safe_name(root["name"]))
            else:
                stack = [("", base)]
                while stack:
                    path, local = stack.pop()
                    offset = 0
                    while True:
                        p = {"public_key": job["url"], "limit": 100, "offset": offset, "preview_size": "1280x"}
                        if path:
                            p["path"] = path
                        m = api_get("", p)
                        items = m.get("_embedded", {}).get("items", [])
                        for it in items:
                            if it["type"] == "dir":
                                stack.append((it["path"], local / safe_name(it["name"])))
                            else:
                                self.add_file(jid, it["path"], it, local / safe_name(it["name"]))
                        if len(items) < 100:
                            break
                        offset += 100
            with self.lock:
                self.db.execute("UPDATE jobs SET status='ready' WHERE id=?", (jid,))
                self.db.commit()
        except Exception as e:
            with self.lock:
                self.db.execute("UPDATE jobs SET status='error', err=? WHERE id=?", (classify(e), jid))
                self.db.commit()

    def add_file(self, jid, rpath, it, target):
        status = "pending"
        size = int(it.get("size") or 0)
        if target.exists() and target.stat().st_size == size:
            status = "ok"
        with self.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO files(job_id, rpath, size, md5, preview, target, status) VALUES(?,?,?,?,?,?,?)",
                (jid, rpath, size, it.get("md5"), it.get("preview"), str(target), status),
            )
            self.db.commit()

    # ---------- scheduling ----------
    def next_file(self):
        with self.lock:
            if self.paused:
                return None
            jobs = [r["id"] for r in self.db.execute("SELECT id FROM jobs ORDER BY id")]
            order = jobs if self.settings["mode"] == "sequential" else sorted(jobs, key=lambda j: self.inflight[j])
            for jid in order:
                row = self.db.execute(
                    "SELECT * FROM files WHERE job_id=? AND status='pending' AND id NOT IN (%s) ORDER BY id LIMIT 1"
                    % (",".join(str(i) for i in self.running) or "0"),
                    (jid,),
                ).fetchone()
                if row:
                    self.running.add(row["id"])
                    self.inflight[jid] += 1
                    return row
                if self.settings["mode"] == "sequential":
                    st = self.db.execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()["status"]
                    if st == "listing":
                        return None
            return None

    def close(self):
        self.stop = True
        self.keep.off()
        for w in self.workers:
            w.join(timeout=10)
        with self.lock:
            try:
                self.db.close()
            except Exception:
                pass

    def ensure_workers(self):
        self.workers = [w for w in self.workers if w.is_alive()]
        for _ in range(self.settings["threads"] - len(self.workers)):
            w = threading.Thread(target=self.worker, daemon=True)
            w.start()
            self.workers.append(w)

    def worker(self):
        idle = 0
        while not self.stop:
            if len([w for w in self.workers if w.is_alive()]) > self.settings["threads"]:
                return
            f = self.next_file()
            if not f:
                idle += 1
                if idle == 3:
                    self.keep.off()
                    self.maybe_auto_retry()
                time.sleep(1)
                continue
            if idle:
                idle = 0
            self.keep.on()
            self.process(f)

    def maybe_auto_retry(self):
        with self.lock:
            if self.auto_retried or self.paused:
                return
            pending = self.db.execute("SELECT COUNT(*) c FROM files WHERE status IN ('pending')").fetchone()["c"]
            listing = self.db.execute("SELECT COUNT(*) c FROM jobs WHERE status='listing'").fetchone()["c"]
            if pending or listing or self.running:
                return
            self.auto_retried = True
            self.retry_failed()

    def retry_failed(self):
        with self.lock:
            self.db.execute("UPDATE files SET status='pending' WHERE status IN ('failed','preview')")
            self.db.commit()

    # ---------- download ----------
    def process(self, f):
        try:
            self.fetch(f)
            self.finish(f, "ok", None)
        except Exception as e:
            reason = classify(e) if not isinstance(e, Fail) else str(e)
            status = "failed"
            if f["preview"] and self.fetch_preview(f):
                status = "preview"
            self.finish(f, status, reason)
            self.errors.append(f"{Path(f['target']).name}  ->  {reason}" + ("  (превью сохранено)" if status == "preview" else ""))

    def finish(self, f, status, err):
        with self.lock:
            self.db.execute("UPDATE files SET status=?, err=?, attempts=attempts+1 WHERE id=?", (status, err, f["id"]))
            self.db.commit()
            self.running.discard(f["id"])
            self.inflight[f["job_id"]] -= 1

    def fetch(self, f):
        target = Path(f["target"])
        job = self.one("SELECT url, dest FROM jobs WHERE id=?", (f["job_id"],))
        if not inside(job["dest"], target):
            raise Fail("PATH")
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and target.stat().st_size == f["size"]:
            return
        params = {"public_key": job["url"]}
        if f["rpath"] != "/":
            params["path"] = f["rpath"]
        href = api_get("/download", params, timeout=self.settings["header_timeout"] + 7, tries=1)["href"]
        part = target.with_name(target.name + ".part")
        have = part.stat().st_size if part.exists() else 0
        headers = {"User-Agent": "yadisk-downloader"}
        if have:
            headers["Range"] = f"bytes={have}-"
        resp = urllib.request.urlopen(urllib.request.Request(href, headers=headers), timeout=self.settings["header_timeout"])
        try:
            resp.fp.raw._sock.settimeout(60)
        except Exception:
            pass
        mode = "ab" if resp.status == 206 else "wb"
        with resp, open(part, mode) as out:
            while True:
                chunk = resp.read(262144)
                if not chunk:
                    break
                out.write(chunk)
                self.bytes_done_session += len(chunk)
        if part.stat().st_size != f["size"]:
            raise Fail("SIZE")
        if f["md5"]:
            h = hashlib.md5()
            with open(part, "rb") as fh:
                for blk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(blk)
            if h.hexdigest() != f["md5"]:
                part.unlink(missing_ok=True)
                raise Fail("MD5")
        os.replace(part, target)

    def fetch_preview(self, f):
        try:
            target = Path(f["target"])
            dest = target.with_name(target.stem + ".preview.jpg")
            req = urllib.request.Request(f["preview"], headers={"User-Agent": "yadisk-downloader"})
            with urllib.request.urlopen(req, timeout=self.settings["header_timeout"] + 7) as r, open(dest, "wb") as out:
                out.write(r.read())
            return True
        except Exception:
            return False

    # ---------- reporting ----------
    def snapshot(self):
        with self.lock:
            now = time.time()
            self.samples.append((now, self.bytes_done_session))
            speed = 0.0
            if len(self.samples) > 1 and self.samples[-1][0] > self.samples[0][0]:
                speed = (self.samples[-1][1] - self.samples[0][1]) / (self.samples[-1][0] - self.samples[0][0])
            jobs = []
            tot = collections.Counter()
            for j in self.db.execute("SELECT * FROM jobs ORDER BY id"):
                c = collections.Counter()
                size_all = size_ok = 0
                for r in self.db.execute("SELECT status, COUNT(*) n, SUM(size) s FROM files WHERE job_id=? GROUP BY status", (j["id"],)):
                    c[r["status"]] = r["n"]
                    size_all += r["s"] or 0
                    if r["status"] == "ok":
                        size_ok += r["s"] or 0
                total = sum(c.values())
                jobs.append({"id": j["id"], "name": j["name"] or j["url"], "url": j["url"], "state": j["status"], "err": j["err"],
                             "total": total, "ok": c["ok"], "failed": c["failed"], "preview": c["preview"],
                             "left": c["pending"], "bytes_all": size_all, "bytes_ok": size_ok})
                for k in ("ok", "failed", "preview", "pending"):
                    tot[k] += c[k]
                tot["bytes_all"] += size_all
                tot["bytes_ok"] += size_ok
            left_bytes = tot["bytes_all"] - tot["bytes_ok"]
            return {"jobs": jobs, "totals": dict(tot), "speed": speed, "eta": (left_bytes / speed) if speed > 1 else None,
                    "paused": self.paused, "settings": self.settings, "running": len(self.running),
                    "errors": list(self.errors)[-15:]}

    def failed_csv(self):
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["file", "reason", "status", "attempts"])
        with self.lock:
            for r in self.db.execute("SELECT target, err, status, attempts FROM files WHERE status IN ('failed','preview') ORDER BY id"):
                w.writerow([r["target"], r["err"], r["status"], r["attempts"]])
        return out.getvalue()


class Handler(BaseHTTPRequestHandler):
    engine = None
    port = 8765

    def log_message(self, *a):
        pass

    def _host_ok(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        return host in ("127.0.0.1", "localhost", "[::1]")

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if not self._host_ok():
            return self._send(403, "forbidden", "text/plain")
        if self.path in ("/", "/index.html"):
            return self._send(200, (HERE / "index.html").read_bytes(), "text/html")
        if self.path == "/api/state":
            return self._send(200, json.dumps(self.engine.snapshot()))
        if self.path == "/api/failed.csv":
            return self._send(200, self.engine.failed_csv(), "text/csv")
        self._send(404, "not found", "text/plain")

    def do_POST(self):
        if not self._host_ok():
            return self._send(403, "forbidden", "text/plain")
        origin = self.headers.get("Origin")
        if origin and urllib.parse.urlparse(origin).hostname not in ("127.0.0.1", "localhost", "::1"):
            return self._send(403, "forbidden", "text/plain")
        n = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return self._send(400, '{"error":"bad json"}')
        e = self.engine
        if self.path == "/api/add":
            added = e.add_links(body.get("links", ""), body.get("dest") or None)
            return self._send(200, json.dumps({"added": added}))
        if self.path == "/api/pause":
            e.paused = True
        elif self.path == "/api/resume":
            e.paused = False
            e.ensure_workers()
        elif self.path == "/api/retry":
            e.auto_retried = True
            e.retry_failed()
        elif self.path == "/api/settings":
            e.set_settings(**{k: v for k, v in body.items() if k in ("threads", "mode", "dest", "header_timeout")})
        else:
            return self._send(404, "{}")
        self._send(200, "{}")


def main():
    ap = argparse.ArgumentParser(description="Download public Yandex Disk links with a local web UI")
    ap.add_argument("links", nargs="*", help="public links (optional, you can add them in the web UI)")
    ap.add_argument("--dest", default=str(Path.home() / "Downloads" / "YandexDisk"))
    ap.add_argument("--threads", type=int, default=10)
    ap.add_argument("--mode", choices=["parallel", "sequential"], default="parallel")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--data", default=str(Path.home() / ".yadisk-downloader"))
    ap.add_argument("--no-browser", action="store_true", help="do not open the browser automatically")
    ap.add_argument("--no-ui", action="store_true", help="run headless, exit when everything is finished")
    a = ap.parse_args()

    eng = Engine(a.data, a.dest, a.threads, a.mode)
    eng.resume_unfinished()
    if a.links:
        eng.add_links(" ".join(a.links), a.dest)
    if a.no_ui:
        try:
            while True:
                s = eng.snapshot()
                t = s["totals"]
                print(f"ok={t.get('ok',0)} failed={t.get('failed',0)} left={t.get('pending',0)} speed={s['speed']/1048576:.2f} MB/s", flush=True)
                if not t.get("pending") and not eng.running and not any(j["state"] == "listing" for j in s["jobs"]) and eng.auto_retried:
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            pass
        return
    Handler.engine = eng
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    url = f"http://127.0.0.1:{a.port}"
    print(f"Open {url}  (Ctrl+C to stop)")
    if not a.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        eng.close()


if __name__ == "__main__":
    main()

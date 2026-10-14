import hashlib, json, os, sys, tempfile, threading, time, unittest, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

FILES = {
    "/photos/a.jpg": b"A" * 5000,
    "/photos/b.jpg": b"B" * 7000,       # download always 500, has preview
    "/photos/c.jpg": b"C" * 3000,       # md5 mismatch served
    "/docs/d.txt": b"hello world",
}
FLAKY = {"/photos/b.jpg"}
RANGES = []


class Fake(BaseHTTPRequestHandler):
    port = 0

    def log_message(self, *a): pass

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        if u.path == "/blob":
            p = q["p"]
            if p in FLAKY:
                self.send_response(500); self.end_headers(); return
            data = FILES[p]
            if p == "/photos/c.jpg":
                data = b"X" * len(data)
            rng = self.headers.get("Range")
            RANGES.append(rng)
            if rng:
                start = int(rng.split("=")[1].split("-")[0])
                part = data[start:]
                self.send_response(206); self.send_header("Content-Length", str(len(part))); self.end_headers(); self.wfile.write(part); return
            self.send_response(200); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data); return
        if u.path == "/preview":
            self.send_response(200); self.end_headers(); self.wfile.write(b"JPEGPREVIEW"); return
        if u.path == "/download":
            href = f"http://127.0.0.1:{Fake.port}/blob?p={urllib.parse.quote(q['path'])}"
            body = json.dumps({"href": href}).encode()
        else:
            path = q.get("path", "")
            if not path:
                items = [{"type": "dir", "name": "photos", "path": "/photos"}, {"type": "dir", "name": "docs", "path": "/docs"}]
                body = json.dumps({"type": "dir", "name": "Root", "_embedded": {"items": items}}).encode()
            else:
                items = []
                for p, d in FILES.items():
                    if p.startswith(path + "/"):
                        it = {"type": "file", "name": p.split("/")[-1], "path": p, "size": len(d), "md5": hashlib.md5(d).hexdigest()}
                        if p in FLAKY:
                            it["preview"] = f"http://127.0.0.1:{Fake.port}/preview"
                        items.append(it)
                body = json.dumps({"type": "dir", "name": path.strip("/"), "_embedded": {"items": items}}).encode()
        self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)


class EngineTest(unittest.TestCase):
    def test_full_flow(self):
        srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
        Fake.port = srv.server_address[1]
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        os.environ["YADISK_API"] = f"http://127.0.0.1:{Fake.port}"
        import importlib, yadisk
        importlib.reload(yadisk)
        with tempfile.TemporaryDirectory() as tmp:
            eng = yadisk.Engine(Path(tmp) / "data", Path(tmp) / "out", threads=4)
            added = eng.add_links("https://disk.yandex.ru/d/TESTLINK1 junk https://disk.yandex.ru/d/TESTLINK1")
            self.assertEqual(len(added), 1)
            for _ in range(100):
                s = eng.snapshot()
                t = s["totals"]
                if t.get("pending", 0) == 0 and not eng.running and s["jobs"] and s["jobs"][0]["state"] == "ready" and sum(t.get(k, 0) for k in ("ok", "failed", "preview")) == 4:
                    break
                time.sleep(0.2)
            out = Path(tmp) / "out" / "Root"
            self.assertEqual((out / "photos" / "a.jpg").read_bytes(), FILES["/photos/a.jpg"])
            self.assertEqual((out / "docs" / "d.txt").read_bytes(), FILES["/docs/d.txt"])
            self.assertFalse((out / "photos" / "b.jpg").exists())
            self.assertEqual((out / "photos" / "b.preview.jpg").read_bytes(), b"JPEGPREVIEW")
            self.assertFalse((out / "photos" / "c.jpg").exists())  # md5 mismatch -> not accepted
            self.assertFalse(list(out.rglob("*.part")))
            t = eng.snapshot()["totals"]
            self.assertEqual((t["ok"], t["preview"], t["failed"]), (2, 1, 1))
            self.assertIn("MD5", eng.failed_csv())
            self.assertIn("HTTP 500", eng.failed_csv())
            eng.close()
        srv.shutdown()

    def test_resume_part_file(self):
        srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
        Fake.port = srv.server_address[1]
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        os.environ["YADISK_API"] = f"http://127.0.0.1:{Fake.port}"
        import importlib, yadisk
        importlib.reload(yadisk)
        RANGES.clear()
        with tempfile.TemporaryDirectory() as tmp:
            part = Path(tmp) / "out" / "Root" / "photos" / "a.jpg.part"
            part.parent.mkdir(parents=True)
            part.write_bytes(FILES["/photos/a.jpg"][:2000])          # interrupted earlier at 2000 bytes
            eng = yadisk.Engine(Path(tmp) / "data", Path(tmp) / "out", threads=2)
            eng.add_links("https://disk.yandex.ru/d/TESTLINK1")
            for _ in range(100):
                if (Path(tmp) / "out" / "Root" / "photos" / "a.jpg").exists():
                    break
                time.sleep(0.2)
            self.assertEqual((Path(tmp) / "out" / "Root" / "photos" / "a.jpg").read_bytes(), FILES["/photos/a.jpg"])
            self.assertIn("bytes=2000-", RANGES)                      # continued, not restarted
            eng.close()
        srv.shutdown()

    def test_restart_keeps_progress(self):
        srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
        Fake.port = srv.server_address[1]
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        os.environ["YADISK_API"] = f"http://127.0.0.1:{Fake.port}"
        import importlib, yadisk
        importlib.reload(yadisk)
        with tempfile.TemporaryDirectory() as tmp:
            eng = yadisk.Engine(Path(tmp) / "data", Path(tmp) / "out", threads=2)
            eng.add_links("https://disk.yandex.ru/d/TESTLINK1")
            for _ in range(100):
                if eng.snapshot()["totals"].get("ok", 0) >= 2:
                    break
                time.sleep(0.2)
            eng.close()
            RANGES.clear()
            eng2 = yadisk.Engine(Path(tmp) / "data", Path(tmp) / "out", threads=2)   # "restart the program"
            before = eng2.snapshot()["totals"]
            self.assertGreaterEqual(before.get("ok", 0), 2)           # finished files remembered
            eng2.close()
        srv.shutdown()

    def test_helpers(self):
        import yadisk
        self.assertEqual(yadisk.safe_name('a/b:c*.jpg'), 'a_b_c_.jpg')
        self.assertEqual(yadisk.safe_name('..'), '_')
        self.assertTrue(yadisk.inside('/tmp/x', '/tmp/x/y/z'))
        self.assertFalse(yadisk.inside('/tmp/x', '/tmp/x/../etc/passwd'))
        self.assertEqual(len(yadisk.LINK_RE.findall("https://yadi.sk/d/abc https://disk.yandex.ru/d/-9F_x https://evil.com/d/q")), 2)


if __name__ == "__main__":
    unittest.main()

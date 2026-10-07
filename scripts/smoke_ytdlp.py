"""Exercise production yt-dlp arguments against loopback fixtures only."""
import http.server
import json
import pathlib
import subprocess
import tempfile
import threading
import functools


def run(*args):
    return subprocess.check_output(["yt-dlp", "--js-runtimes", "deno", *args], text=True)


with tempfile.TemporaryDirectory() as root:
    root = pathlib.Path(root)
    (root / "fixture.mp4").write_bytes(b"local smoke fixture\n")
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(root)),
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/fixture.mp4"
    info = root / "fixture.info.json"
    info.write_text(json.dumps({
        "id": "fixture", "title": "fixture", "webpage_url": url,
        "url": url, "ext": "mp4", "extractor": "generic",
        "extractor_key": "Generic", "filesize": 20,
    }))
    try:
        discovered = run("--flat-playlist", "--skip-download", "--no-warnings",
                         "--print", "%(webpage_url)s", "--load-info-json", str(info))
        assert discovered.strip() == url
        metadata = json.loads(run("--dump-single-json", "--skip-download", "--no-warnings",
                                  "--load-info-json", str(info)))
        assert metadata["id"] == "fixture"
        downloaded = run("--no-playlist", "--restrict-filenames", "--max-filesize", "1024",
                         "--print", "after_move:filepath", "-o", str(root / "out.%(ext)s"),
                         "--", url)
        assert pathlib.Path(downloaded.strip()).read_bytes() == b"local smoke fixture\n"
        # Exercise the JavaScript runtime under the image's non-root identity.
        assert subprocess.check_output(["deno", "eval", "console.log(1+1)"], text=True).strip() == "2"
        print("Discovery, metadata, download arguments, and Deno passed.")
    finally:
        server.shutdown()

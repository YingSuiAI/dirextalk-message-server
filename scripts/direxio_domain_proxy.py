#!/usr/bin/env python3
import http.client
import argparse
import ssl
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


ROUTES = {
    "a.ai": ("127.0.0.1", 18008),
    "b.ai": ("127.0.0.1", 28008),
    "c.ai": ("127.0.0.1", 38008),
}


class DirexioDomainProxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self._proxy()

    def do_POST(self):
        self._proxy()

    def do_PUT(self):
        self._proxy()

    def do_DELETE(self):
        self._proxy()

    def do_PATCH(self):
        self._proxy()

    def do_OPTIONS(self):
        self._proxy()

    def _proxy(self):
        parsed = urlsplit(self.path)
        host_header = parsed.netloc or self.headers.get("Host", "")
        hostname = host_header.split("@")[-1].split(":")[0].lower()
        target = ROUTES.get(hostname)
        if target is None:
            self.send_error(502, f"unknown host: {hostname}")
            return

        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        body = None
        content_length = self.headers.get("Content-Length")
        if content_length:
            body = self.rfile.read(int(content_length))

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower()
            not in {"host", "connection", "proxy-connection", "keep-alive", "transfer-encoding"}
        }
        headers["Host"] = f"{hostname}:{target[1]}"

        conn = http.client.HTTPConnection(target[0], target[1], timeout=120)
        try:
            conn.request(self.command, path, body=body, headers=headers)
            response = conn.getresponse()
        except Exception as exc:
            self.send_error(502, str(exc))
            return

        self.send_response(response.status, response.reason)
        has_content_length = False
        for key, value in response.getheaders():
            lower_key = key.lower()
            if lower_key in {"connection", "transfer-encoding", "server", "date"}:
                continue
            if lower_key == "content-length":
                has_content_length = True
            self.send_header(key, value)
        if not has_content_length:
            self.send_header("Connection", "close")
        self.end_headers()
        try:
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("port", nargs="?", type=int, default=8888)
    parser.add_argument("--cert")
    parser.add_argument("--key")
    args = parser.parse_args()

    port = args.port
    server = ThreadingHTTPServer(("127.0.0.1", port), DirexioDomainProxy)
    scheme = "http"
    if args.cert and args.key:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(args.cert, args.key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        scheme = "https"
    print(f"direxio domain proxy listening on {scheme}://127.0.0.1:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

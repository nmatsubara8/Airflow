"""軽量 HTTP サーバーユーティリティ（metrics / dashboard 共用）。"""
from __future__ import annotations

import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any

logger = logging.getLogger(__name__)

_HTTP_AUTH_TOKEN_ENV = "AIRFLOW_HTTP_AUTH_TOKEN"


def _expected_token() -> str | None:
    v = os.environ.get(_HTTP_AUTH_TOKEN_ENV, "").strip()
    return v if v else None


class _BaseHandler(BaseHTTPRequestHandler):
    """GET / POST リクエストを処理する最小 HTTP ハンドラ基底クラス。

    サブクラスは ``handle_get`` / ``handle_post`` を実装すること。
    戻り値: (status_code, content_type, body_text)
    """

    _server_name: str = "http"
    _cors_enabled: bool = False

    def _check_auth(self) -> bool:
        import hmac
        expected = _expected_token()
        if expected is None:
            return True
        auth_header = self.headers.get("Authorization", "")
        bearer = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
        return bool(bearer) and hmac.compare_digest(bearer, expected)

    def do_GET(self) -> None:  # noqa: N802
        from urllib.parse import parse_qs, urlparse
        if not self._check_auth():
            self._send(401, "text/plain", "Unauthorized")
            return
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        code, content_type, text = self.handle_get(parsed.path, qs)
        self._send(code, content_type, text)

    def do_POST(self) -> None:  # noqa: N802
        from urllib.parse import parse_qs, urlparse
        if not self._check_auth():
            self._send(401, "text/plain", "Unauthorized")
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length > 0 else ""
        parsed = urlparse(self.path)
        form = {k: v[0] for k, v in parse_qs(body).items()}
        code, content_type, text = self.handle_post(parsed.path, form)
        if code >= 0:
            self._send(code, content_type, text)

    def _send(self, code: int, content_type: str, text: str) -> None:
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if self._cors_enabled:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def handle_get(self, path: str, qs: dict[str, list[str]]) -> tuple[int, str, str]:
        return 404, "text/plain", "Not Found"

    def handle_post(self, path: str, form: dict[str, str]) -> tuple[int, str, str]:
        return 405, "text/plain", "Method Not Allowed"

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: ANN401
        logger.debug("%s: " + fmt, self._server_name, *args)


def start_server(
    handler_cls: type[BaseHTTPRequestHandler],
    port: int,
    thread_name: str,
) -> HTTPServer:
    httpd = HTTPServer(("0.0.0.0", port), handler_cls)
    Thread(target=httpd.serve_forever, daemon=True, name=thread_name).start()
    logger.info("%s started on :%d", thread_name, port)
    return httpd

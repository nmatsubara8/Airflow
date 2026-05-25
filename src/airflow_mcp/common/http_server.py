"""軽量 HTTP サーバーユーティリティ（metrics / dashboard 共用）。"""
from __future__ import annotations

import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any

logger = logging.getLogger(__name__)


class _BaseHandler(BaseHTTPRequestHandler):
    """GET リクエストを処理する最小 HTTP ハンドラ基底クラス。

    サブクラスは ``handle_get(path, qs) -> tuple[int, str, str]`` を実装すること。
    戻り値: (status_code, content_type, body_text)
    """

    _server_name: str = "http"

    def do_GET(self) -> None:  # noqa: N802
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        code, content_type, text = self.handle_get(parsed.path, qs)
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_get(
        self,
        path: str,
        qs: dict[str, list[str]],
    ) -> tuple[int, str, str]:
        """サブクラスでオーバーライドする。(status, content_type, body) を返す。"""
        return 404, "text/plain", "Not Found"

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: ANN401
        logger.debug("%s: " + fmt, self._server_name, *args)


def start_server(
    handler_cls: type[BaseHTTPRequestHandler],
    port: int,
    thread_name: str,
) -> HTTPServer:
    """HTTPServer をデーモンスレッドで起動して返す。"""
    httpd = HTTPServer(("0.0.0.0", port), handler_cls)
    Thread(target=httpd.serve_forever, daemon=True, name=thread_name).start()
    logger.info("%s started on :%d", thread_name, port)
    return httpd

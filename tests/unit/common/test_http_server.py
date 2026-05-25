"""common/http_server.py の単体テスト。"""
from __future__ import annotations

import socket
import urllib.error
import urllib.request

from airflow_mcp.common.http_server import _BaseHandler, start_server


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _EchoHandler(_BaseHandler):
    _server_name = "test-echo"

    def handle_get(self, path, qs):
        if path == "/hello":
            return 200, "text/plain", "world"
        return 404, "text/plain", "Not Found"


def test_base_handler_default_returns_404():
    """デフォルトの handle_get() はオーバーライドなしで 404 を返すこと。"""
    port = _free_port()
    httpd = start_server(_BaseHandler, port, "test-default")
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/anything", timeout=5)
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        httpd.shutdown()


def test_subclass_handler_returns_200():
    """サブクラスの handle_get() が 200 を返すこと。"""
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-echo")
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/hello", timeout=5) as resp:
            assert resp.status == 200
            assert resp.read().decode() == "world"
    finally:
        httpd.shutdown()


def test_subclass_handler_unknown_path_returns_404():
    """サブクラスの未知パスで 404 が返ること。"""
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-echo2")
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/unknown", timeout=5)
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        httpd.shutdown()


def test_start_server_returns_httpserver():
    """start_server が HTTPServer インスタンスを返すこと。"""
    from http.server import HTTPServer
    port = _free_port()
    httpd = start_server(_BaseHandler, port, "test-return")
    try:
        assert isinstance(httpd, HTTPServer)
    finally:
        httpd.shutdown()


def test_query_string_passed_to_handle_get():
    """クエリ文字列が qs dict として handle_get に渡されること。"""
    received: dict = {}

    class _QSHandler(_BaseHandler):
        _server_name = "test-qs"

        def handle_get(self, path, qs):
            received.update(qs)
            return 200, "text/plain", "ok"

    port = _free_port()
    httpd = start_server(_QSHandler, port, "test-qs")
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/?foo=bar&x=1", timeout=5)
        assert received.get("foo") == ["bar"]
        assert received.get("x") == ["1"]
    finally:
        httpd.shutdown()

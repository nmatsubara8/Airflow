"""common/http_server.py の単体テスト。"""
from __future__ import annotations

import socket
import urllib.error
import urllib.request

import pytest

from airflow_mcp.common.http_server import _BaseHandler, start_server

pytestmark = pytest.mark.slow


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


# ── HTTP Bearer 認証 ──────────────────────────────────────

def test_auth_token_required_returns_401_without_header(monkeypatch):
    """AIRFLOW_HTTP_AUTH_TOKEN 設定時、Authorization ヘッダーなしで 401 が返ること。"""
    monkeypatch.setenv("AIRFLOW_HTTP_AUTH_TOKEN", "secret123")
    # モジュール側キャッシュをリセット（環境変数参照は動的なので不要だが念のため）
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-auth")
    try:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/hello", timeout=5)
            assert False, "Expected 401"
        except urllib.error.HTTPError as e:
            assert e.code == 401
    finally:
        httpd.shutdown()
    monkeypatch.delenv("AIRFLOW_HTTP_AUTH_TOKEN", raising=False)


def test_auth_token_correct_returns_200(monkeypatch):
    """正しい Bearer トークンで 200 が返ること。"""
    monkeypatch.setenv("AIRFLOW_HTTP_AUTH_TOKEN", "secret123")
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-auth-ok")
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/hello",
            headers={"Authorization": "Bearer secret123"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()
    monkeypatch.delenv("AIRFLOW_HTTP_AUTH_TOKEN", raising=False)


def test_auth_token_wrong_returns_401(monkeypatch):
    """誤った Bearer トークンで 401 が返ること。"""
    monkeypatch.setenv("AIRFLOW_HTTP_AUTH_TOKEN", "secret123")
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-auth-bad")
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/hello",
            headers={"Authorization": "Bearer wrongtoken"},
        )
        try:
            urllib.request.urlopen(req, timeout=5)
            assert False, "Expected 401"
        except urllib.error.HTTPError as e:
            assert e.code == 401
    finally:
        httpd.shutdown()
    monkeypatch.delenv("AIRFLOW_HTTP_AUTH_TOKEN", raising=False)


def test_auth_token_empty_env_skips_auth(monkeypatch):
    """AIRFLOW_HTTP_AUTH_TOKEN が空文字のとき認証スキップで 200 が返ること。"""
    monkeypatch.setenv("AIRFLOW_HTTP_AUTH_TOKEN", "")
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-auth-empty")
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/hello", timeout=5) as resp:
            assert resp.status == 200
    finally:
        httpd.shutdown()


def test_base_post_auth_failure_returns_401(monkeypatch):
    """POST で認証失敗すると 401 が返ること（base do_POST の auth check）。"""
    monkeypatch.setenv("AIRFLOW_HTTP_AUTH_TOKEN", "secret123")
    import airflow_mcp.common.http_server as hs
    monkeypatch.setattr(hs, "_expected_token", lambda: "secret123")
    port = _free_port()
    httpd = start_server(_EchoHandler, port, "test-post-auth")
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/action",
            data=b"x=1",
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=5)
            assert False, "Expected 401"
        except urllib.error.HTTPError as e:
            assert e.code == 401
    finally:
        httpd.shutdown()
    monkeypatch.delenv("AIRFLOW_HTTP_AUTH_TOKEN", raising=False)


def test_base_post_default_handle_post_returns_405():
    """handle_post() をオーバーライドしないとき POST で 405 が返ること。"""
    port = _free_port()
    httpd = start_server(_BaseHandler, port, "test-post-default")
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/action",
            data=b"x=1",
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=5)
            assert False, "Expected 405"
        except urllib.error.HTTPError as e:
            assert e.code == 405
    finally:
        httpd.shutdown()

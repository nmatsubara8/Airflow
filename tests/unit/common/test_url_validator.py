"""common/url_validator.py の単体テスト（SSRF LOW 対策）。"""
from __future__ import annotations

import pytest

from airflow_mcp.common.url_validator import validate_ollama_url, validate_slack_webhook_url


# ── validate_ollama_url ───────────────────────────────────

class TestValidateOllamaUrl:

    def test_localhost_http_allowed(self):
        assert validate_ollama_url("http://localhost:11434") == "http://localhost:11434"

    def test_localhost_https_allowed(self):
        assert validate_ollama_url("https://localhost:11434") == "https://localhost:11434"

    def test_loopback_ip_allowed(self):
        assert validate_ollama_url("http://127.0.0.1:11434") == "http://127.0.0.1:11434"

    def test_private_lan_allowed(self):
        assert validate_ollama_url("http://192.168.1.100:11434") == "http://192.168.1.100:11434"

    def test_hostname_allowed(self):
        assert validate_ollama_url("http://ollama.internal:11434") == "http://ollama.internal:11434"

    def test_https_external_allowed(self):
        assert validate_ollama_url("https://ollama.example.com") == "https://ollama.example.com"

    # ── 拒否ケース ──────────────────────────────────────

    def test_invalid_scheme_file_raises(self):
        with pytest.raises(ValueError, match="スキーム"):
            validate_ollama_url("file:///etc/passwd")

    def test_invalid_scheme_ftp_raises(self):
        with pytest.raises(ValueError, match="スキーム"):
            validate_ollama_url("ftp://localhost:11434")

    def test_invalid_scheme_gopher_raises(self):
        with pytest.raises(ValueError, match="スキーム"):
            validate_ollama_url("gopher://localhost/1")

    def test_aws_metadata_ip_blocked(self):
        with pytest.raises(ValueError, match="ブロック"):
            validate_ollama_url("http://169.254.169.254/latest/meta-data/")

    def test_gcp_metadata_hostname_blocked(self):
        with pytest.raises(ValueError, match="ブロック"):
            validate_ollama_url("http://metadata.google.internal/computeMetadata/v1/")

    def test_zero_host_blocked(self):
        with pytest.raises(ValueError, match="ブロック"):
            validate_ollama_url("http://0.0.0.0:11434")

    def test_link_local_ipv6_blocked(self):
        with pytest.raises(ValueError, match="ブロック"):
            validate_ollama_url("http://[fe80::1]:11434")

    def test_link_local_ipv4_blocked(self):
        with pytest.raises(ValueError, match="ブロック"):
            validate_ollama_url("http://169.254.1.1:8080")

    def test_no_hostname_raises(self):
        with pytest.raises(ValueError, match="ホスト名"):
            validate_ollama_url("http://")


# ── validate_slack_webhook_url ────────────────────────────

class TestValidateSlackWebhookUrl:

    def test_valid_hooks_slack_com(self):
        url = "https://hooks.slack.com/services/T000/B000/xxxx"
        assert validate_slack_webhook_url(url) == url

    def test_valid_subdomain_slack_com(self):
        url = "https://company.slack.com/webhooks/xxx"
        assert validate_slack_webhook_url(url) == url

    def test_http_scheme_returns_none(self, caplog):
        result = validate_slack_webhook_url("http://hooks.slack.com/services/xxx")
        assert result is None
        assert "https" in caplog.text

    def test_non_slack_hostname_returns_none(self, caplog):
        result = validate_slack_webhook_url("https://evil.example.com/webhook")
        assert result is None
        assert "slack.com" in caplog.text

    def test_metadata_endpoint_returns_none(self, caplog):
        result = validate_slack_webhook_url("https://169.254.169.254/latest/meta-data")
        assert result is None
        assert "slack.com" in caplog.text

    def test_file_scheme_returns_none(self, caplog):
        result = validate_slack_webhook_url("file:///etc/passwd")
        assert result is None

    def test_lookalike_domain_returns_none(self, caplog):
        result = validate_slack_webhook_url("https://evil-slack.com/webhook")
        assert result is None
        assert "slack.com" in caplog.text

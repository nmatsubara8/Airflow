"""SSRF 対策 URL バリデーター（DDD-08 §4.2）。

外部 HTTP エンドポイント（OLLAMA_URL, AIRFLOW_SLACK_WEBHOOK 等）を
使用前に検証し、クラウドメタデータサービスや不正スキームへのリクエストを防ぐ。
"""
from __future__ import annotations

import ipaddress
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# 既知のクラウドメタデータエンドポイント（SSRF でよく悪用される）
_BLOCKED_HOSTNAMES: frozenset[str] = frozenset({
    "169.254.169.254",       # AWS / Azure / GCP instance metadata
    "metadata.google.internal",  # GCP
    "metadata.internal",     # 汎用エイリアス
    "0.0.0.0",               # catch-all
})


def _is_ssrf_blocked(hostname: str) -> bool:
    """SSRF の観点でブロックすべきホストかどうかを返す。"""
    h = hostname.lower().strip("[]")  # IPv6 ブラケット除去
    if h in _BLOCKED_HOSTNAMES:
        return True
    try:
        addr = ipaddress.ip_address(h)
        # リンクローカル (169.254.0.0/16, fe80::/10) はメタデータ経路として使われる
        if addr.is_link_local:
            return True
    except ValueError:
        pass
    return False


def validate_ollama_url(url: str) -> str:
    """OLLAMA_URL を検証して返す。

    許可:
      - スキーム: http または https
      - ホスト: localhost / 127.0.0.1 / プライベート LAN アドレス / 任意のホスト名
    拒否:
      - http/https 以外のスキーム
      - クラウドメタデータエンドポイント（169.254.169.254 等）
      - リンクローカルアドレス（fe80::, 169.254.*）
      - ホスト名なし

    Raises:
        ValueError: URL が不正な場合。
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"OLLAMA_URL: スキームは http または https である必要があります（got {parsed.scheme!r}）"
        )
    hostname = parsed.hostname or ""
    if not hostname:
        raise ValueError("OLLAMA_URL: ホスト名が必要です")
    if _is_ssrf_blocked(hostname):
        raise ValueError(
            f"OLLAMA_URL: ブロックされたホスト {hostname!r}（クラウドメタデータ / リンクローカル）"
        )
    return url


def validate_slack_webhook_url(url: str) -> str | None:
    """AIRFLOW_SLACK_WEBHOOK を検証して返す。

    許可:
      - スキーム: https のみ
      - ホスト名: *.slack.com で終わること

    不正な場合は None を返す（通知をスキップ、システムは継続稼働）。
    """
    parsed = urlparse(url)
    if parsed.scheme != "https":
        logger.warning(
            "AIRFLOW_SLACK_WEBHOOK: スキームは https である必要があります（got %r）— 通知をスキップ",
            parsed.scheme,
        )
        return None
    hostname = (parsed.hostname or "").lower()
    if not hostname.endswith(".slack.com") and hostname != "slack.com":
        logger.warning(
            "AIRFLOW_SLACK_WEBHOOK: ホスト名は *.slack.com である必要があります（got %r）— 通知をスキップ",
            hostname,
        )
        return None
    return url

---
title: APIレート制限の実装
tags: [engineering, backend, api]
priority: 3
section: engineering
features:
  required_accuracy: medium
  latency_sensitivity: batch
---

# APIレート制限の実装

## 背景

現在のAPIエンドポイントにはレート制限が実装されておらず、
過負荷攻撃に対して脆弱な状態です。

## 要件

- 1分間あたり最大100リクエスト
- IPアドレスおよびAPIキー単位での制限
- 超過時は429 Too Many Requestsを返す
- X-RateLimit-* ヘッダーを全レスポンスに付与

## 実装方針

Redis + Sliding Window Counterアルゴリズムを採用する。

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests
from loguru import logger

COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
PAGE_SIZE = int(os.getenv("COINGECKO_PAGE_SIZE", "250"))
RATE_LIMIT = int(os.getenv("COINGECKO_RATE_LIMIT", "30"))
PAGES_PER_CYCLE = int(os.getenv("COINGECKO_PAGES_PER_CYCLE", "5"))

FIELDS = [
    "id", "symbol", "name", "current_price", "market_cap",
    "total_volume", "price_change_24h", "price_change_percentage_24h",
    "market_cap_rank", "high_24h", "low_24h",
]


def fetch_markets(page: int = 1, vs_currency: str = "usd") -> list[dict]:
    url = f"{COINGECKO_BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": PAGE_SIZE,
        "page": page,
        "sparkline": "false",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def enrich_record(coin: dict) -> dict:
    now = datetime.now(timezone.utc)
    return {
        **{k: coin.get(k) for k in FIELDS},
        "ingestion_ts": now.isoformat(),
        "dt": now.strftime("%Y-%m-%d"),
        "hour": now.strftime("%H"),
    }


def save_batch_local(records: list[dict], output_dir: str) -> str:
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%S")
    path = os.path.join(output_dir, f"batch_{ts}.ndjson")
    os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return path


def run_cycle(output_dir: str = "/tmp/crypto_bronze"):
    all_records = []
    interval = 60.0 / RATE_LIMIT

    for page in range(1, PAGES_PER_CYCLE + 1):
        try:
            coins = fetch_markets(page=page)
            enriched = [enrich_record(c) for c in coins]
            all_records.extend(enriched)
            logger.info(f"Página {page}: {len(coins)} moedas coletadas")
        except requests.RequestException as exc:
            logger.error(f"Erro na página {page}: {exc}")

        if page < PAGES_PER_CYCLE:
            time.sleep(interval)

    if all_records:
        path = save_batch_local(all_records, output_dir)
        logger.info(f"Batch salvo: {path} ({len(all_records)} registros)")

    return all_records


if __name__ == "__main__":
    logger.info("Iniciando coleta CoinGecko (ciclo único)")
    run_cycle()

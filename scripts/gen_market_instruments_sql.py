#!/usr/bin/env python3
"""
Generate INSERT SQL for new FXCM market instruments.

Typical usage (from repo root, with .env present and fxcm-api running):

  python scripts/gen_market_instruments_sql.py --count 20
  python scripts/gen_market_instruments_sql.py --count 20 --mix forex:8,crypto:7,stocks:5
  python scripts/gen_market_instruments_sql.py --symbols GER30,UK100,SOL/USD
  python scripts/gen_market_instruments_sql.py --list-candidates --count 30
  python scripts/gen_market_instruments_sql.py --picklist scripts/my_symbols.txt

Requires: sqlalchemy (project venv). python-dotenv is optional.
Reads existing symbols from DB, fetches metadata from FXCM sidecar, writes a .sql file.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    env_path = ROOT / ".env"
    try:
        from dotenv import load_dotenv

        load_dotenv(env_path)
        return
    except ImportError:
        pass

    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_env()

DEFAULT_FXCM_URL = os.getenv("FXCM_API_BASE_URL", "http://127.0.0.1:8100").rstrip("/")
DEFAULT_DB_URL = os.getenv("DATABASE_URL_SYNC") or os.getenv("database_url_sync")

MARKET_BUCKETS = ("forex", "crypto", "stocks")
DEFAULT_SEARCH_KEYWORDS = (
    "GER30",
    "UK100",
    "JPN225",
    "FRA40",
    "US2000",
    "Copper",
    "NGAS",
)

INTERVALS = (
    ("1min", 10),
    ("5min", 20),
    ("15min", 30),
    ("30min", 40),
    ("1h", 50),
    ("2h", 60),
    ("4h", 70),
    ("8h", 80),
    ("1day", 90),
    ("1week", 100),
)

PLAN_RANK = {"T": 0, "V": 1, "D": 2}
BASKET_RE = re.compile(
    r"^(AIRLINES|BIOTECH|CANNABIS|CASINOS|CRYPTOSTOCK|CRYPTOMAJOR|CHN\.)",
    re.IGNORECASE,
)


def normalize_symbol(value: str) -> str:
    return "".join(ch for ch in value.upper() if ch.isalnum())


def sql_str(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def parse_mix(raw: str | None) -> dict[str, int] | None:
    if not raw:
        return None
    mix: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Invalid --mix segment {part!r}, expected market:count")
        market, count_text = part.split(":", 1)
        market = market.strip().casefold()
        if market not in MARKET_BUCKETS and market != "search":
            raise ValueError(f"Unknown market bucket {market!r}")
        mix[market] = int(count_text.strip())
    return mix


def default_mix(count: int, markets: list[str]) -> dict[str, int]:
    if len(markets) == 1:
        return {markets[0]: count}
    weights = {"forex": 4, "crypto": 3, "stocks": 3, "search": 2}
    total_w = sum(weights.get(m, 1) for m in markets)
    mix: dict[str, int] = {}
    assigned = 0
    for i, market in enumerate(markets):
        if i == len(markets) - 1:
            mix[market] = count - assigned
        else:
            n = max(0, round(count * weights.get(market, 1) / total_w))
            mix[market] = n
            assigned += n
    while sum(mix.values()) < count:
        mix[markets[0]] = mix.get(markets[0], 0) + 1
    while sum(mix.values()) > count:
        for market in reversed(markets):
            if mix.get(market, 0) > 0:
                mix[market] -= 1
                if sum(mix.values()) == count:
                    break
    return mix


def http_get_json(url: str, *, timeout: float) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise SystemExit(f"HTTP {exc.code} for {url}\n{body}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Failed to reach {url}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Unexpected JSON from {url}")
    return payload


def fetch_market_catalog(fxcm_url: str, *, timeout: float, outputsize: int) -> dict[str, dict]:
    catalog: dict[str, dict] = {}
    for market in MARKET_BUCKETS:
        qs = urllib.parse.urlencode({"market": market, "outputsize": outputsize})
        data = http_get_json(f"{fxcm_url}/symbols/market?{qs}", timeout=timeout)
        for item in data.get("items") or []:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol") or item.get("provider_symbol")
            if not symbol:
                continue
            catalog[str(symbol)] = {**item, "_bucket": market}
    return prefer_us_stock_symbols(catalog)


def prefer_us_stock_symbols(catalog: dict[str, dict]) -> dict[str, dict]:
    us_bases: set[str] = set()
    for symbol in catalog:
        if symbol.endswith(".us"):
            us_bases.add(symbol.rsplit(".", 1)[0].upper())

    filtered: dict[str, dict] = {}
    for symbol, item in catalog.items():
        if symbol.endswith(".ext"):
            base = symbol.rsplit(".", 1)[0].upper()
            if base in us_bases:
                continue
        filtered[symbol] = item
    return filtered


def fetch_search_catalog(
    fxcm_url: str,
    keywords: list[str],
    *,
    timeout: float,
) -> dict[str, dict]:
    catalog: dict[str, dict] = {}
    for keyword in keywords:
        qs = urllib.parse.urlencode({"keyword": keyword, "outputsize": 5})
        data = http_get_json(f"{fxcm_url}/symbols/search?{qs}", timeout=timeout)
        items = data.get("items") or []
        if not items:
            continue
        item = items[0]
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol") or item.get("provider_symbol")
        if not symbol:
            continue
        catalog[str(symbol)] = {**item, "_bucket": "search"}
    return catalog


def load_existing_from_db(database_url: str) -> tuple[set[str], int]:
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:
        raise SystemExit(
            "sqlalchemy is required. Run with the project virtualenv, e.g.\n"
            "  pip install -r requirements.txt"
        ) from exc

    engine = create_engine(database_url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT normalized_symbol, symbol
                FROM market_instrument
                WHERE provider = 'FXCM'
                """
            )
        ).fetchall()
        next_sort = conn.execute(
            text("SELECT COALESCE(MAX(sort_weight), -1) + 1 FROM market_instrument")
        ).scalar_one()
    existing = {str(row[0]) for row in rows if row[0]}
    existing |= {normalize_symbol(str(row[1])) for row in rows if row[1]}
    return existing, int(next_sort)


def is_basket_symbol(symbol: str) -> bool:
    if BASKET_RE.match(symbol):
        return True
    if "/" in symbol or "." in symbol:
        return False
    return symbol.isupper() and symbol.isalpha() and len(symbol) > 5


def candidate_rank(item: dict) -> tuple:
    plan = str(item.get("provider_plan") or "Z")
    return (
        PLAN_RANK.get(plan, 9),
        str(item.get("symbol") or ""),
    )


def list_candidates(
    catalog: dict[str, dict],
    existing: set[str],
    *,
    exclude: set[str],
) -> list[tuple[str, dict]]:
    candidates: list[tuple[str, dict]] = []
    for symbol, item in catalog.items():
        norm = normalize_symbol(symbol)
        if not norm or norm in existing or symbol in exclude:
            continue
        if is_basket_symbol(symbol):
            continue
        candidates.append((symbol, item))
    candidates.sort(key=lambda pair: (candidate_rank(pair[1]), pair[0]))
    return candidates


def pick_by_mix(
    grouped: dict[str, list[tuple[str, dict]]],
    mix: dict[str, int],
) -> list[str]:
    picked: list[str] = []
    for bucket, count in mix.items():
        pool = grouped.get(bucket, [])
        for symbol, _item in pool[:count]:
            if symbol not in picked:
                picked.append(symbol)
    return picked


def load_picklist(path: Path) -> list[str]:
    symbols: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.append(line.split("#", 1)[0].strip())
    return symbols


def build_sql(
    symbols: list[str],
    catalog: dict[str, dict],
    *,
    sort_weight_start: int,
    target_history_bars: int,
    fxcm_url: str,
) -> str:
    lines = [
        f"-- FXCM market instruments INSERT script",
        f"-- Generated at {datetime.now(timezone.utc).isoformat()}",
        f"-- Source: {fxcm_url}/symbols/market (+ optional /symbols/search)",
        f"-- Symbols ({len(symbols)}): {', '.join(symbols)}",
        "-- After running:",
        "--   POST /api_v1/admin/market-data/sync?mode=all&force_due=true",
        "BEGIN;",
        "",
        "-- 1) market_instrument",
    ]

    for idx, symbol in enumerate(symbols):
        item = catalog[symbol]
        norm = normalize_symbol(symbol)
        prov = str(item.get("provider_symbol") or symbol)
        prov_norm = normalize_symbol(prov)
        sort_weight = sort_weight_start + idx
        source_payload = json.dumps(
            {"search_item": item, "quote": None, "profile": None},
            ensure_ascii=False,
        ).replace("'", "''")

        lines.append(
            f"INSERT INTO market_instrument (\n"
            f"  provider, symbol, normalized_symbol, provider_symbol, normalized_provider_symbol,\n"
            f"  name, display_label, exchange, mic_code, market, asset_type, country, currency,\n"
            f"  exchange_timezone, provider_plan, sort_weight,\n"
            f"  is_active, is_searchable, is_hot, supports_history, supports_quote,\n"
            f"  source_payload, metadata_synced_at, last_seen_at\n"
            f") SELECT\n"
            f"  'FXCM', {sql_str(symbol)}, {sql_str(norm)}, {sql_str(prov)}, {sql_str(prov_norm)},\n"
            f"  {sql_str(item.get('name') or symbol)}, {sql_str(item.get('label'))}, {sql_str(item.get('exchange'))}, NULL,\n"
            f"  {sql_str(item.get('market'))}, {sql_str(item.get('asset_type'))}, {sql_str(item.get('country'))}, {sql_str(item.get('currency'))},\n"
            f"  {sql_str(item.get('timezone'))}, {sql_str(item.get('provider_plan'))}, {sort_weight},\n"
            f"  TRUE, TRUE, TRUE, TRUE, TRUE,\n"
            f"  '{source_payload}'::jsonb, NOW(), NOW()\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM market_instrument WHERE normalized_symbol = {sql_str(norm)});"
        )
        lines.append("")

    lines.append("-- 2) market_instrument_alias")
    for symbol in symbols:
        item = catalog[symbol]
        norm = normalize_symbol(symbol)
        prov = str(item.get("provider_symbol") or symbol)
        aliases: list[tuple[str, str, int]] = [
            (symbol, "CANONICAL", 1),
            (prov, "PROVIDER", 2),
            (symbol.replace("/", ""), "SEARCH", 30),
        ]
        label = item.get("label")
        if label and label not in (symbol, prov):
            aliases.append((str(label), "SEARCH", 40))

        for alias, alias_type, priority in aliases:
            alias_norm = normalize_symbol(alias)
            if not alias_norm:
                continue
            lines.append(
                f"INSERT INTO market_instrument_alias (instrument_id, alias, normalized_alias, alias_type, priority, is_active)\n"
                f"SELECT mi.id, {sql_str(alias)}, {sql_str(alias_norm)}, {sql_str(alias_type)}, {priority}, TRUE\n"
                f"FROM market_instrument mi\n"
                f"WHERE mi.normalized_symbol = {sql_str(norm)}\n"
                f"  AND NOT EXISTS (SELECT 1 FROM market_instrument_alias a WHERE a.normalized_alias = {sql_str(alias_norm)});"
            )
        lines.append("")

    lines.append(f"-- 3) market_bar_sync_state ({len(INTERVALS)} intervals each)")
    for symbol in symbols:
        norm = normalize_symbol(symbol)
        for interval, priority in INTERVALS:
            lines.append(
                f"INSERT INTO market_bar_sync_state (\n"
                f"  instrument_id, provider, interval, price_type, enabled, priority,\n"
                f"  target_history_bars, sync_mode, backfill_completed, last_status, retry_count\n"
                f") SELECT\n"
                f"  mi.id, 'FXCM', {sql_str(interval)}, 'mid', TRUE, {priority},\n"
                f"  {target_history_bars}, 'BACKFILL', FALSE, 'IDLE', 0\n"
                f"FROM market_instrument mi\n"
                f"WHERE mi.normalized_symbol = {sql_str(norm)}\n"
                f"  AND NOT EXISTS (\n"
                f"    SELECT 1 FROM market_bar_sync_state s\n"
                f"    WHERE s.instrument_id = mi.id AND s.interval = {sql_str(interval)} AND s.price_type = 'mid'\n"
                f"  );"
            )
        lines.append("")

    lines.append("COMMIT;")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate INSERT SQL for new FXCM market instruments.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/gen_market_instruments_sql.py --count 20
  python scripts/gen_market_instruments_sql.py --count 20 --mix forex:8,crypto:7,stocks:5
  python scripts/gen_market_instruments_sql.py --symbols GER30,SOL/USD,AAPL.us
  python scripts/gen_market_instruments_sql.py --list-candidates --count 50
  python scripts/gen_market_instruments_sql.py --picklist scripts/my_symbols.txt
        """.strip(),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=20,
        help="How many new symbols to auto-pick when --symbols/--picklist not set (default: 20)",
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated explicit symbol list (skips auto-pick)",
    )
    parser.add_argument(
        "--picklist",
        type=Path,
        help="Text file with one symbol per line (# comments allowed)",
    )
    parser.add_argument(
        "--markets",
        default="forex,crypto,stocks",
        help="Market buckets for auto-pick (default: forex,crypto,stocks)",
    )
    parser.add_argument(
        "--mix",
        help="Per-bucket counts, e.g. forex:8,crypto:7,stocks:5,search:2",
    )
    parser.add_argument(
        "--search",
        help="Extra /symbols/search keywords (comma-separated). "
        "Default: include common indices/commodities when auto-picking.",
    )
    parser.add_argument(
        "--no-default-search",
        action="store_true",
        help="Do not auto-fetch GER30/UK100/Copper etc. via /symbols/search",
    )
    parser.add_argument(
        "--exclude",
        help="Comma-separated symbols to exclude from auto-pick",
    )
    parser.add_argument(
        "--fxcm-url",
        default=DEFAULT_FXCM_URL,
        help=f"FXCM sidecar base URL (default: {DEFAULT_FXCM_URL})",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DB_URL,
        help="SQLAlchemy sync DB URL (default: DATABASE_URL_SYNC from .env)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output .sql path (default: scripts/insert_market_instruments_<timestamp>.sql)",
    )
    parser.add_argument(
        "--outputsize",
        type=int,
        default=200,
        help="FXCM /symbols/market outputsize per bucket (default: 200)",
    )
    parser.add_argument(
        "--target-history-bars",
        type=int,
        default=1000,
        help="target_history_bars for sync states (default: 1000)",
    )
    parser.add_argument(
        "--sort-weight-start",
        type=int,
        help="First sort_weight value (default: max existing + 1 from DB)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout seconds for FXCM API (default: 120)",
    )
    parser.add_argument(
        "--list-candidates",
        action="store_true",
        help="Print candidate symbols and exit without writing SQL",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit(
            "Missing database URL. Set DATABASE_URL_SYNC in .env or pass --database-url."
        )

    fxcm_url = args.fxcm_url.rstrip("/")
    exclude = {s.strip() for s in (args.exclude or "").split(",") if s.strip()}

    print(f"Loading existing instruments from DB...")
    existing, next_sort = load_existing_from_db(args.database_url)
    print(f"  Found {len(existing)} normalized symbols in market_instrument")

    markets = [m.strip().casefold() for m in args.markets.split(",") if m.strip()]
    for market in markets:
        if market not in MARKET_BUCKETS and market != "search":
            raise SystemExit(f"Unsupported market bucket: {market}")

    print(f"Fetching FXCM catalog from {fxcm_url} ...")
    catalog = fetch_market_catalog(
        fxcm_url, timeout=args.timeout, outputsize=args.outputsize
    )

    search_keywords: list[str] = []
    if args.search:
        search_keywords = [k.strip() for k in args.search.split(",") if k.strip()]
    elif not args.no_default_search and ("search" in markets or not args.symbols):
        search_keywords = list(DEFAULT_SEARCH_KEYWORDS)

    if search_keywords:
        print(f"Searching FXCM for: {', '.join(search_keywords)}")
        catalog.update(
            fetch_search_catalog(fxcm_url, search_keywords, timeout=args.timeout)
        )

    candidates = list_candidates(catalog, existing, exclude=exclude)
    grouped: dict[str, list[tuple[str, dict]]] = {m: [] for m in MARKET_BUCKETS + ("search",)}
    for symbol, item in candidates:
        bucket = str(item.get("_bucket") or "search")
        grouped.setdefault(bucket, []).append((symbol, item))

    if args.list_candidates:
        print(f"\nCandidates ({len(candidates)} total, excluding DB + baskets):\n")
        for i, (symbol, item) in enumerate(candidates[: max(args.count, 50)], 1):
            bucket = item.get("_bucket", "?")
            plan = item.get("provider_plan", "?")
            market = item.get("market", "?")
            print(f"  {i:3}. {symbol:16} bucket={bucket} plan={plan} market={market}")
        if len(candidates) > max(args.count, 50):
            print(f"  ... and {len(candidates) - max(args.count, 50)} more")
        return

    if args.picklist:
        selected = load_picklist(args.picklist)
    elif args.symbols:
        selected = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        mix = parse_mix(args.mix)
        if mix is None:
            active_markets = [m for m in markets if m != "search"]
            if search_keywords and "search" not in active_markets:
                active_markets = active_markets + ["search"]
            mix = default_mix(args.count, active_markets or list(MARKET_BUCKETS))
        selected = pick_by_mix(grouped, mix)
        if len(selected) < args.count:
            for symbol, _item in candidates:
                if symbol not in selected:
                    selected.append(symbol)
                if len(selected) >= args.count:
                    break

    missing = [s for s in selected if s not in catalog]
    already = [s for s in selected if normalize_symbol(s) in existing]
    if missing:
        raise SystemExit(f"Symbols not found in FXCM catalog: {missing}")
    if already:
        print(f"Warning: these are already in DB and will be skipped by SQL: {already}")

    sort_start = args.sort_weight_start if args.sort_weight_start is not None else next_sort
    sql = build_sql(
        selected,
        catalog,
        sort_weight_start=sort_start,
        target_history_bars=args.target_history_bars,
        fxcm_url=fxcm_url,
    )

    if args.output:
        out_path = args.output
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = ROOT / "scripts" / f"insert_market_instruments_{stamp}.sql"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(sql, encoding="utf-8")

    print(f"\nSelected {len(selected)} symbols:")
    for symbol in selected:
        item = catalog[symbol]
        print(f"  - {symbol} ({item.get('market')}, plan={item.get('provider_plan')})")
    print(f"\nWrote {out_path} ({len(sql):,} bytes)")
    print("Run the SQL against your DB, then trigger backfill via admin sync API.")


if __name__ == "__main__":
    main()

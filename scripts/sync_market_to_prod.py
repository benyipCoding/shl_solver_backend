#!/usr/bin/env python3
"""
Push local FXCM market data (instruments, aliases, OHLCV bars) to a remote PostgreSQL.

Typical workflow (local machine with SSH tunnel to prod PG):

  # 1) One-time full copy into empty remote DB (after alembic upgrade on remote)
  python scripts/sync_market_to_prod.py bootstrap

  # 2) Scheduled incremental push (Task Scheduler every 5-15 min)
  python scripts/sync_market_to_prod.py incremental

  # 3) Inspect local vs remote counts
  python scripts/sync_market_to_prod.py status

Environment (.env):
  DATABASE_URL_SYNC                  local source DB (required)
  MARKET_REPLICA_DATABASE_URL_SYNC   remote target DB via SSH tunnel, e.g.
    postgresql+psycopg://user:pass@127.0.0.1:15432/shl_solver

Optional:
  MARKET_REPLICA_BATCH_SIZE=2000
  MARKET_REPLICA_OVERLAP_BARS=2

This script is standalone and does not start the FastAPI app or FXCM scheduler.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


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


def resolve_database_urls(
    *,
    source_url: str | None,
    target_url: str | None,
) -> tuple[str, str]:
    resolved_source = (
        source_url
        or os.getenv("DATABASE_URL_SYNC")
        or os.getenv("database_url_sync")
    )
    resolved_target = (
        target_url
        or os.getenv("MARKET_REPLICA_DATABASE_URL_SYNC")
        or os.getenv("market_replica_database_url_sync")
    )
    if not resolved_source:
        raise SystemExit(
            "Missing source database URL. Set DATABASE_URL_SYNC in .env or pass --source-url."
        )
    if not resolved_target:
        raise SystemExit(
            "Missing target database URL. Set MARKET_REPLICA_DATABASE_URL_SYNC in .env "
            "or pass --target-url."
        )
    if resolved_source == resolved_target:
        raise SystemExit("Source and target database URLs must be different.")
    return resolved_source, resolved_target


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync local market data to remote PostgreSQL (CLI only)."
    )
    parser.add_argument(
        "mode",
        choices=("bootstrap", "incremental", "metadata", "bars", "status"),
        help=(
            "bootstrap=one-time full copy; incremental=metadata+bars; "
            "metadata=instruments+aliases only; bars=OHLCV only; status=counts"
        ),
    )
    parser.add_argument(
        "--source-url",
        default=None,
        help="Local SQLAlchemy sync URL (default: DATABASE_URL_SYNC)",
    )
    parser.add_argument(
        "--target-url",
        default=None,
        help="Remote SQLAlchemy sync URL (default: MARKET_REPLICA_DATABASE_URL_SYNC)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("MARKET_REPLICA_BATCH_SIZE", "2000")),
        help="Rows per batch when copying or upserting bars",
    )
    parser.add_argument(
        "--overlap-bars",
        type=int,
        default=int(os.getenv("MARKET_REPLICA_OVERLAP_BARS", "2")),
        help="Re-push last N bars per interval to heal gaps",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="bootstrap only: truncate remote market tables before copy",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON result",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    source_url, target_url = resolve_database_urls(
        source_url=args.source_url,
        target_url=args.target_url,
    )

    from app.services.market_data_replica import MarketDataReplicaService

    service = MarketDataReplicaService(
        source_url,
        target_url,
        batch_size=args.batch_size,
        overlap_bars=args.overlap_bars,
    )

    exit_code = 0
    try:
        if args.mode == "status":
            payload = service.get_status()
            if args.json:
                print(json.dumps(payload, default=str, indent=2))
            else:
                for label in ("source", "target"):
                    counts = payload[label]
                    print(
                        f"[{label}] instruments={counts['instrument_count']} "
                        f"aliases={counts['alias_count']} bars={counts['bar_count']} "
                        f"latest_bar_time={counts['latest_bar_time']}"
                    )
            return exit_code

        if args.mode == "bootstrap":
            result = service.run_bootstrap(force=args.force)
        elif args.mode == "incremental":
            result = service.run_incremental()
        elif args.mode == "metadata":
            from app.services.market_data_replica.types import MarketReplicaResult

            result = MarketReplicaResult(mode="metadata")
            try:
                counts = service.sync_metadata()
                result.instruments_upserted = counts["instruments"]
                result.aliases_upserted = counts["aliases"]
            except Exception as exc:
                logging.exception("Metadata sync failed")
                result.errors.append(f"{type(exc).__name__}: {exc}")
                exit_code = 1
        else:
            from app.services.market_data_replica.types import MarketReplicaResult

            result = MarketReplicaResult(mode="bars")
            try:
                result.bars_upserted = service.sync_bars_incremental()
            except Exception as exc:
                logging.exception("Bar sync failed")
                result.errors.append(f"{type(exc).__name__}: {exc}")
                exit_code = 1

        if args.json:
            print(json.dumps(result.to_summary(), default=str, indent=2))
        else:
            print(
                f"mode={result.mode} skipped={result.skipped} "
                f"instruments={result.instruments_upserted} "
                f"aliases={result.aliases_upserted} bars={result.bars_upserted}"
            )
            if result.errors:
                for error in result.errors:
                    print(f"error: {error}", file=sys.stderr)

        if result.errors or result.skipped:
            exit_code = 1 if result.errors else 0
    finally:
        service.close()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

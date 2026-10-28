#!/usr/bin/env python3
"""
删除 market_ohlcv_bar 中最近一段时间（默认近一年）的全部 K 线，并重置受影响的同步状态。

背景：旧的增量采集可能写入了尚未收盘的 K 线。删除后，定时历史采集会从剩余数据的
最新水位重新向前采集到历史截止时间。

用法（默认只统计不删除）：

  # 1) 先看要删多少（dry run）
  python scripts/purge_recent_bars.py --since 2025-10-28

  # 2) 确认后执行，会要求输入 DELETE 二次确认
  python scripts/purge_recent_bars.py --since 2025-10-28 --execute

  # 通过 SSH 隧道清理生产库（MARKET_REPLICA_DATABASE_URL_SYNC）
  python scripts/purge_recent_bars.py --since 2025-10-28 --target replica --execute

  # 在生产服务器的容器内执行（容器的 DATABASE_URL_SYNC 指向生产库）
  docker compose run --rm backend python scripts/purge_recent_bars.py --since 2025-10-28 --execute

本地和生产请使用同一个 --since，并先清理本地、再清理生产：
sync_market_to_prod.py incremental 按生产库的最新 K 线向后推送，
若先清理生产而本地仍有旧数据，下一次推送会把旧数据推回去。

Environment (.env):
  DATABASE_URL_SYNC                  默认目标库
  MARKET_REPLICA_DATABASE_URL_SYNC   --target replica 时使用
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.engine import Engine, make_url  # noqa: E402

PROVIDER = "FXCM"
SUPPORTED_INTERVALS = (
    "1min", "5min", "15min", "30min", "1h", "2h", "3h", "4h", "6h", "8h", "1day", "1week",
)
BACKFILL_EARLIEST_DATE = datetime(1994, 1, 1, tzinfo=timezone.utc)
FORWARD_CURSOR_KEY = "history_forward_until"


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
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def resolve_url(args: argparse.Namespace) -> str:
    if args.database_url:
        return args.database_url
    names = (
        ("MARKET_REPLICA_DATABASE_URL_SYNC", "market_replica_database_url_sync")
        if args.target == "replica"
        else ("DATABASE_URL_SYNC", "database_url_sync")
    )
    for name in names:
        if os.getenv(name):
            return os.environ[name]
    raise SystemExit(f"缺少数据库地址：请设置 {names[0]} 或传入 --database-url")


def parse_since(value: str | None) -> datetime:
    if not value:
        # 默认按 UTC 日期取整到一年前当天 00:00，便于本地与生产使用同一个边界。
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=365)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def series_intervals(engine: Engine) -> list[str]:
    """K 线表很大，不做 DISTINCT 全表扫描；周期取支持列表与同步状态中出现过的周期。"""
    with engine.connect() as conn:
        state_intervals = conn.execute(
            text("SELECT DISTINCT interval FROM market_bar_sync_state")
        ).scalars().all()
    return list(dict.fromkeys([*SUPPORTED_INTERVALS, *state_intervals]))


def count_rows(engine: Engine, intervals: list[str], since: datetime) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for interval in intervals:
            counts[interval] = conn.execute(
                text(
                    "SELECT count(*) FROM market_ohlcv_bar "
                    "WHERE provider = :provider AND interval = :interval AND bar_time >= :since"
                ),
                {"provider": PROVIDER, "interval": interval, "since": since},
            ).scalar_one()
    return counts


def delete_rows(engine: Engine, intervals: list[str], since: datetime, batch_size: int) -> int:
    """按周期分批删除并逐批提交，避免单个超大事务长时间持锁、撑爆 WAL。"""
    total = 0
    for interval in intervals:
        deleted_for_interval = 0
        while True:
            with engine.begin() as conn:
                deleted = conn.execute(
                    text(
                        "DELETE FROM market_ohlcv_bar WHERE id IN ("
                        " SELECT id FROM market_ohlcv_bar"
                        " WHERE provider = :provider AND interval = :interval AND bar_time >= :since"
                        " LIMIT :batch_size)"
                    ),
                    {"provider": PROVIDER, "interval": interval, "since": since, "batch_size": batch_size},
                ).rowcount
            if not deleted:
                break
            deleted_for_interval += deleted
            total += deleted
            print(f"  {interval:>6}: 已删除 {deleted_for_interval:,}", flush=True)
    return total


def reset_states(engine: Engine, since: datetime) -> int:
    """按剩余 K 线重算受影响状态的水位，并重新排入定时历史采集。

    hydrate 只会把水位往后推，不会回退，因此删除后必须在这里显式回退。
    """
    reset = 0
    with engine.begin() as conn:
        states = conn.execute(
            text(
                "SELECT id, instrument_id, interval, price_type, earliest_synced_bar_time, meta "
                "FROM market_bar_sync_state "
                "WHERE provider = :provider AND ("
                " latest_synced_bar_time >= :since OR sync_mode <> 'BACKFILL'"
                " OR meta::jsonb ? :cursor_key)"
            ),
            {"provider": PROVIDER, "since": since, "cursor_key": FORWARD_CURSOR_KEY},
        ).mappings().all()

        for state in states:
            series = {
                "instrument_id": state["instrument_id"],
                "interval": state["interval"],
                "price_type": state["price_type"],
                "provider": PROVIDER,
            }
            series_filter = (
                "instrument_id = :instrument_id AND interval = :interval "
                "AND price_type = :price_type AND provider = :provider"
            )
            latest = conn.execute(
                text(f"SELECT max(bar_time) FROM market_ohlcv_bar WHERE {series_filter}"), series
            ).scalar()
            earliest = state["earliest_synced_bar_time"]
            if latest is None:
                earliest = None
            elif earliest is None or earliest >= since:
                earliest = conn.execute(
                    text(
                        f"SELECT min(bar_time) FROM market_ohlcv_bar WHERE {series_filter} "
                        "AND bar_time >= :floor"
                    ),
                    {**series, "floor": BACKFILL_EARLIEST_DATE},
                ).scalar() or latest

            meta = dict(state["meta"]) if isinstance(state["meta"], dict) else None
            if meta is not None:
                meta.pop(FORWARD_CURSOR_KEY, None)

            conn.execute(
                text(
                    "UPDATE market_bar_sync_state SET "
                    " latest_synced_bar_time = :latest,"
                    " earliest_synced_bar_time = :earliest,"
                    " backfill_completed = (backfill_completed AND :keep_backfill),"
                    " sync_mode = 'BACKFILL', next_sync_from = now(),"
                    " last_status = 'IDLE', last_error = NULL, retry_count = 0,"
                    " meta = CAST(:meta AS json), updated_at = now() "
                    "WHERE id = :id"
                ),
                {
                    "id": state["id"],
                    "latest": latest,
                    "earliest": earliest,
                    # 该序列已无任何 K 线时，回补也要重新开始。
                    "keep_backfill": latest is not None,
                    "meta": None if meta is None else json.dumps(meta),
                },
            )
            reset += 1
    return reset


def main() -> int:
    # 控制台编码不支持中文时（如 cp1252）不要因输出报错中断删除流程。
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    load_env()
    parser = argparse.ArgumentParser(description="删除近一段时间的 K 线并重置同步状态")
    parser.add_argument("--since", help="删除 bar_time >= 该时间的 K 线（ISO，默认按 UTC 一年前当天 00:00）")
    parser.add_argument("--target", choices=("local", "replica"), default="local",
                        help="local 用 DATABASE_URL_SYNC，replica 用 MARKET_REPLICA_DATABASE_URL_SYNC")
    parser.add_argument("--database-url", help="显式指定数据库地址，优先于 --target")
    parser.add_argument("--batch-size", type=int, default=50000, help="每批删除行数")
    parser.add_argument("--execute", action="store_true", help="真正执行删除；不加则只统计")
    parser.add_argument("--yes", action="store_true", help="跳过输入 DELETE 的二次确认")
    args = parser.parse_args()

    since = parse_since(args.since)
    url = resolve_url(args)
    engine = create_engine(url)
    try:
        return purge(args, since, url, engine)
    finally:
        engine.dispose()


def purge(args: argparse.Namespace, since: datetime, url: str, engine: Engine) -> int:
    target = make_url(url)
    print(f"目标数据库：{target.host}:{target.port or 5432}/{target.database}")
    print(f"删除范围：bar_time >= {since.isoformat()}（全部品种、全部周期）")

    intervals = series_intervals(engine)
    counts = count_rows(engine, intervals, since)
    for interval in intervals:
        if counts[interval]:
            print(f"  {interval:>6}: {counts[interval]:,} 行")
    total = sum(counts.values())
    print(f"合计：{total:,} 行")

    if not args.execute:
        print("\n当前为 dry run，未删除任何数据。确认无误后加 --execute 执行。")
        return 0
    if total == 0:
        print("没有需要删除的 K 线。")
    elif not args.yes:
        answer = input(f"\n即将删除 {target.database} 中 {total:,} 行 K 线，输入 DELETE 确认：")
        if answer.strip() != "DELETE":
            print("已取消。")
            return 1

    deleted = delete_rows(engine, intervals, since, max(1000, args.batch_size)) if total else 0
    reset = reset_states(engine, since)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("ANALYZE market_ohlcv_bar"))
    print(f"\n完成：删除 {deleted:,} 行 K 线，重置 {reset} 条同步状态。")
    print("这些状态会在定时历史采集中从剩余数据的最新水位重新向前采集到历史截止时间。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

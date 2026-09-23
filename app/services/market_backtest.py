from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market_data import (
    MarketBacktestEvent,
    MarketBacktestSession,
    MarketBacktestTrade,
    MarketInstrument,
)
from app.models.user import User
from app.schemas.market_backtest import (
    BacktestEventCreate,
    BacktestSessionComplete,
    BacktestSessionCreate,
)
from app.services.fxcm_market_sync.utils import to_decimal
from app.services.market_master import market_master_service


class BacktestPersistError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


_SIDE_ALIASES = {
    "BUY": "BUY",
    "SELL": "SELL",
    "LONG": "BUY",
    "SHORT": "SELL",
}

_CLOSE_REASON_ALIASES = {
    "SL_HIT": "SL_HIT",
    "SL HIT": "SL_HIT",
    "TP_HIT": "TP_HIT",
    "TP HIT": "TP_HIT",
    "MARKET_CLOSE": "MARKET_CLOSE",
    "MARKET CLOSE": "MARKET_CLOSE",
    "FORCED_MARKET_CLOSE": "FORCED_MARKET_CLOSE",
    "FORCED MARKET CLOSE": "FORCED_MARKET_CLOSE",
}


class MarketBacktestService:
    async def create_session(
        self,
        db: AsyncSession,
        user: User,
        payload: BacktestSessionCreate,
    ) -> dict:
        if payload.client_session_id:
            existing = await self._get_session_by_client_id(
                db, user.id, payload.client_session_id
            )
            if existing is not None:
                return self._serialize_session(existing)

        instrument = await self._resolve_instrument(db, payload.symbol)
        interval = market_master_service._resolve_interval_name(payload.interval)
        start_bar_time = self._parse_bar_time(payload.start_bar_time)
        cursor_bar_time = (
            self._parse_bar_time(payload.cursor_bar_time)
            if payload.cursor_bar_time is not None
            else start_bar_time
        )
        initial_balance = to_decimal(payload.initial_balance) or Decimal("10000")

        session = MarketBacktestSession(
            public_id=str(uuid4()),
            client_session_id=payload.client_session_id,
            user_id=user.id,
            instrument_id=instrument.id,
            symbol=instrument.symbol,
            interval=interval,
            timeframe=payload.timeframe,
            provider=instrument.provider or "FXCM",
            price_type=payload.price_type or "mid",
            source="BACKTEST",
            status="RUNNING",
            visibility="PRIVATE",
            start_bar_time=start_bar_time,
            start_bar_index=payload.start_bar_index,
            initial_visible_bars=payload.initial_visible_bars,
            cursor_bar_time=cursor_bar_time,
            cursor_bar_index=payload.cursor_bar_index
            if payload.cursor_bar_index is not None
            else payload.start_bar_index,
            initial_balance=initial_balance,
            trade_count=0,
            closed_trade_count=0,
            win_count=0,
            realized_pnl=Decimal("0"),
        )
        db.add(session)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            if payload.client_session_id:
                existing = await self._get_session_by_client_id(
                    db, user.id, payload.client_session_id
                )
                if existing is not None:
                    return self._serialize_session(existing)
            raise
        await db.refresh(session)
        return self._serialize_session(session)

    async def record_event(
        self,
        db: AsyncSession,
        user: User,
        public_id: str,
        payload: BacktestEventCreate,
    ) -> dict:
        session = await self._get_owned_session(db, user.id, public_id, for_update=True)
        if session.status != "RUNNING":
            raise BacktestPersistError(409, "回测场次已结束，无法再写入成交")

        bar_time = self._parse_bar_time(payload.bar_time)
        self._bump_cursor(session, bar_time, payload.bar_index)

        if payload.event_type == "OPEN":
            result = await self._open_trade(db, session, payload, bar_time)
        elif payload.event_type in {"MODIFY_SL", "MODIFY_TP"}:
            result = await self._modify_trade(db, session, payload, bar_time)
        else:
            result = await self._close_trade(db, session, payload, bar_time)

        await db.commit()
        return result

    async def complete_session(
        self,
        db: AsyncSession,
        user: User,
        public_id: str,
        payload: BacktestSessionComplete,
    ) -> dict:
        session = await self._get_owned_session(db, user.id, public_id, for_update=True)
        cursor_bar_time = (
            self._parse_bar_time(payload.cursor_bar_time)
            if payload.cursor_bar_time is not None
            else session.cursor_bar_time
        )
        if cursor_bar_time is not None:
            self._bump_cursor(session, cursor_bar_time, payload.cursor_bar_index)

        if session.status == "RUNNING":
            # 从未下单的回测没有回放价值，软删除以免占历史名额
            if (session.trade_count or 0) <= 0:
                now = datetime.now(timezone.utc)
                session.deleted_at = now
                session.status = "ABANDONED"
                session.ended_at = now
                await db.commit()
                await db.refresh(session)
                return self._serialize_session(session)

            mark_price = to_decimal(payload.mark_price)
            if (
                mark_price is not None
                and mark_price > 0
                and cursor_bar_time is not None
            ):
                await self._force_close_open_trades(
                    db,
                    session,
                    mark_price=mark_price,
                    bar_time=cursor_bar_time,
                    bar_index=payload.cursor_bar_index,
                )
            session.status = "COMPLETED"
            session.ended_at = datetime.now(timezone.utc)
            if payload.ending_balance is not None:
                session.ending_balance = to_decimal(payload.ending_balance)
            elif session.ending_balance is None:
                session.ending_balance = (session.initial_balance or Decimal("0")) + (
                    session.realized_pnl or Decimal("0")
                )

        await db.commit()
        await db.refresh(session)
        return self._serialize_session(session)

    async def delete_session(
        self,
        db: AsyncSession,
        user: User,
        public_id: str,
    ) -> dict:
        session = await self._get_session_by_public_id(db, public_id)
        if session is None:
            raise BacktestPersistError(404, "回测场次不存在")
        if not getattr(user, "is_superuser", False) and session.user_id != user.id:
            raise BacktestPersistError(403, "只能删除自己的回测记录")

        now = datetime.now(timezone.utc)
        session.deleted_at = now
        if session.status == "RUNNING":
            session.status = "ABANDONED"
            session.ended_at = now

        await db.execute(
            update(MarketBacktestTrade)
            .where(
                MarketBacktestTrade.session_id == session.id,
                MarketBacktestTrade.deleted_at.is_(None),
            )
            .values(deleted_at=now)
        )
        await db.execute(
            update(MarketBacktestEvent)
            .where(
                MarketBacktestEvent.session_id == session.id,
                MarketBacktestEvent.deleted_at.is_(None),
            )
            .values(deleted_at=now)
        )
        await db.commit()
        return {"public_id": session.public_id, "deleted": True}

    async def list_sessions(
        self,
        db: AsyncSession,
        user: User,
        page: int,
        size: int,
    ) -> dict:
        filters = (
            MarketBacktestSession.user_id == user.id,
            MarketBacktestSession.deleted_at.is_(None),
        )
        count_result = await db.execute(
            select(func.count())
            .select_from(MarketBacktestSession)
            .where(*filters)
        )
        total = int(count_result.scalar() or 0)
        result = await db.execute(
            select(MarketBacktestSession)
            .where(*filters)
            .order_by(MarketBacktestSession.created_at.desc())
            .offset((page - 1) * size)
            .limit(size)
        )
        return {
            "items": [
                self._serialize_session(session)
                for session in result.scalars().all()
            ],
            "total": total,
            "page": page,
            "size": size,
        }

    async def get_session_detail(
        self,
        db: AsyncSession,
        user: User,
        public_id: str,
    ) -> dict:
        session = await self._get_owned_session(db, user.id, public_id)
        trades_result = await db.execute(
            select(MarketBacktestTrade)
            .where(
                MarketBacktestTrade.session_id == session.id,
                MarketBacktestTrade.deleted_at.is_(None),
            )
            .order_by(MarketBacktestTrade.sequence_no.asc())
        )
        events_result = await db.execute(
            select(MarketBacktestEvent, MarketBacktestTrade.client_trade_id)
            .join(
                MarketBacktestTrade,
                MarketBacktestEvent.trade_id == MarketBacktestTrade.id,
            )
            .where(
                MarketBacktestEvent.session_id == session.id,
                MarketBacktestEvent.deleted_at.is_(None),
            )
            .order_by(MarketBacktestEvent.sequence_no.asc())
        )
        payload = self._serialize_session(session)
        payload["trades"] = [
            self._serialize_trade(trade) for trade in trades_result.scalars().all()
        ]
        payload["events"] = [
            self._serialize_event(event, client_trade_id)
            for event, client_trade_id in events_result.all()
        ]
        return payload

    async def _open_trade(
        self,
        db: AsyncSession,
        session: MarketBacktestSession,
        payload: BacktestEventCreate,
        bar_time: datetime,
    ) -> dict:
        existing = await self._get_trade_by_client_id(
            db, session.id, payload.client_trade_id
        )
        if existing is not None:
            return self._serialize_event_result(session, existing, "OPEN", None)

        side = self._normalize_side(payload.side)
        units = payload.units
        entry_price = to_decimal(payload.price)
        if side is None or units is None or entry_price is None:
            raise BacktestPersistError(400, "开仓事件缺少方向、手数或价格")

        next_trade_seq = await self._next_trade_sequence(db, session.id)
        sl_price = to_decimal(payload.sl_price)
        tp_price = to_decimal(payload.tp_price)
        trade = MarketBacktestTrade(
            session_id=session.id,
            user_id=session.user_id,
            instrument_id=session.instrument_id,
            client_trade_id=payload.client_trade_id,
            sequence_no=next_trade_seq,
            symbol=session.symbol,
            interval=session.interval,
            side=side,
            units=units,
            status="OPEN",
            entry_price=entry_price,
            entry_bar_time=bar_time,
            entry_bar_index=payload.bar_index,
            initial_sl_price=sl_price,
            initial_tp_price=tp_price,
            sl_price=sl_price,
            tp_price=tp_price,
        )
        db.add(trade)
        await db.flush()

        event = await self._append_event(
            db,
            session,
            trade,
            event_type="OPEN",
            bar_time=bar_time,
            bar_index=payload.bar_index,
            side=side,
            units=units,
            price=entry_price,
            sl_price=sl_price,
            tp_price=tp_price,
        )
        session.trade_count = (session.trade_count or 0) + 1
        return self._serialize_event_result(session, trade, "OPEN", event.sequence_no)

    async def _modify_trade(
        self,
        db: AsyncSession,
        session: MarketBacktestSession,
        payload: BacktestEventCreate,
        bar_time: datetime,
    ) -> dict:
        trade = await self._require_open_trade(db, session.id, payload.client_trade_id)
        new_price = to_decimal(payload.price)
        if "price" not in payload.model_fields_set:
            raise BacktestPersistError(400, "改价事件缺少价格")

        if payload.event_type == "MODIFY_SL":
            trade.sl_price = new_price
        else:
            trade.tp_price = new_price

        event = await self._append_event(
            db,
            session,
            trade,
            event_type=payload.event_type,
            bar_time=bar_time,
            bar_index=payload.bar_index,
            side=trade.side,
            units=trade.units,
            price=new_price,
            sl_price=trade.sl_price,
            tp_price=trade.tp_price,
        )
        return self._serialize_event_result(
            session, trade, payload.event_type, event.sequence_no
        )

    async def _close_trade(
        self,
        db: AsyncSession,
        session: MarketBacktestSession,
        payload: BacktestEventCreate,
        bar_time: datetime,
    ) -> dict:
        trade = await self._get_trade_by_client_id(
            db, session.id, payload.client_trade_id
        )
        if trade is None:
            raise BacktestPersistError(404, "未找到对应持仓")
        if trade.status == "CLOSED":
            return self._serialize_event_result(session, trade, "CLOSE", None)

        close_price = to_decimal(payload.price)
        if close_price is None:
            raise BacktestPersistError(400, "平仓事件缺少价格")

        event = await self._apply_close(
            db,
            session,
            trade,
            close_price=close_price,
            bar_time=bar_time,
            bar_index=payload.bar_index,
            close_reason=self._normalize_close_reason(payload.close_reason),
            close_units=payload.units,
            client_event_id=payload.client_event_id,
        )
        return self._serialize_event_result(session, trade, "CLOSE", event.sequence_no)

    async def _force_close_open_trades(
        self,
        db: AsyncSession,
        session: MarketBacktestSession,
        *,
        mark_price: Decimal,
        bar_time: datetime,
        bar_index: int | None,
    ) -> None:
        result = await db.execute(
            select(MarketBacktestTrade).where(
                MarketBacktestTrade.session_id == session.id,
                MarketBacktestTrade.status == "OPEN",
                MarketBacktestTrade.deleted_at.is_(None),
            )
        )
        for trade in result.scalars().all():
            await self._apply_close(
                db,
                session,
                trade,
                close_price=mark_price,
                bar_time=bar_time,
                bar_index=bar_index,
                close_reason="FORCED_MARKET_CLOSE",
            )

    async def _apply_close(
        self,
        db: AsyncSession,
        session: MarketBacktestSession,
        trade: MarketBacktestTrade,
        *,
        close_price: Decimal,
        bar_time: datetime,
        bar_index: int | None,
        close_reason: str,
        close_units: int | None = None,
        client_event_id: str | None = None,
    ) -> MarketBacktestEvent:
        # The position keeps its original size; CLOSE events track each fill.
        # This also supports legacy events that closed the entire position.
        result = await db.execute(
            select(MarketBacktestEvent).where(
                MarketBacktestEvent.trade_id == trade.id,
                MarketBacktestEvent.event_type == "CLOSE",
                MarketBacktestEvent.deleted_at.is_(None),
            )
        )
        previous_closes = result.scalars().all()
        if client_event_id:
            for previous in previous_closes:
                if (previous.payload or {}).get("client_event_id") == client_event_id:
                    return previous
        remaining_units = trade.units - sum(event.units or 0 for event in previous_closes)
        units = remaining_units if close_units is None else close_units
        if units <= 0 or units > remaining_units:
            raise BacktestPersistError(400, "平仓数量超出剩余持仓")

        realized_points = (
            close_price - trade.entry_price
            if trade.side == "BUY"
            else trade.entry_price - close_price
        )
        realized_pnl = realized_points * Decimal(units)
        trade.realized_pnl = (trade.realized_pnl or Decimal("0")) + realized_pnl
        if units == remaining_units:
            trade.status = "CLOSED"
            trade.realized_points = trade.realized_pnl / Decimal(trade.units)
            # The final snapshot uses the weighted average fill price; events
            # retain exact fill prices/times for replay and chart markers.
            trade.close_price = trade.entry_price + (
                trade.realized_points if trade.side == "BUY" else -trade.realized_points
            )
            trade.close_bar_time = bar_time
            trade.close_bar_index = bar_index
            trade.close_reason = close_reason
            trade.closed_at = datetime.now(timezone.utc)
            session.closed_trade_count = (session.closed_trade_count or 0) + 1
            if trade.realized_pnl > 0:
                session.win_count = (session.win_count or 0) + 1

        event = await self._append_event(
            db,
            session,
            trade,
            event_type="CLOSE",
            bar_time=bar_time,
            bar_index=bar_index,
            side=trade.side,
            units=units,
            price=close_price,
            sl_price=trade.sl_price,
            tp_price=trade.tp_price,
            close_reason=close_reason,
            event_payload={"client_event_id": client_event_id} if client_event_id else None,
        )
        session.realized_pnl = (session.realized_pnl or Decimal("0")) + realized_pnl
        return event

    async def _append_event(
        self,
        db: AsyncSession,
        session: MarketBacktestSession,
        trade: MarketBacktestTrade,
        *,
        event_type: str,
        bar_time: datetime,
        bar_index: int | None,
        side: str | None,
        units: int | None,
        price: Decimal | None,
        sl_price: Decimal | None,
        tp_price: Decimal | None,
        close_reason: str | None = None,
        event_payload: dict | None = None,
    ) -> MarketBacktestEvent:
        sequence_no = await self._next_event_sequence(db, session.id)
        event = MarketBacktestEvent(
            session_id=session.id,
            trade_id=trade.id,
            sequence_no=sequence_no,
            event_type=event_type,
            bar_time=bar_time,
            bar_index=bar_index,
            side=side,
            units=units,
            price=price,
            sl_price=sl_price,
            tp_price=tp_price,
            close_reason=close_reason,
            payload=event_payload,
        )
        db.add(event)
        await db.flush()
        return event

    async def _resolve_instrument(
        self, db: AsyncSession, symbol: str
    ) -> MarketInstrument:
        normalized_symbol = market_master_service._normalize_lookup(symbol)
        if not normalized_symbol:
            raise BacktestPersistError(400, "缺少有效的交易品种")

        result = await db.execute(
            select(MarketInstrument).where(
                MarketInstrument.deleted_at.is_(None),
                or_(
                    MarketInstrument.normalized_symbol == normalized_symbol,
                    MarketInstrument.normalized_provider_symbol == normalized_symbol,
                    MarketInstrument.symbol == symbol,
                ),
            )
        )
        instrument = result.scalars().first()
        if instrument is None:
            raise BacktestPersistError(400, f"未找到交易品种 {symbol}")
        return instrument

    async def _get_session_by_public_id(
        self, db: AsyncSession, public_id: str, *, for_update: bool = False
    ) -> MarketBacktestSession | None:
        statement = select(MarketBacktestSession).where(
            MarketBacktestSession.public_id == public_id,
            MarketBacktestSession.deleted_at.is_(None),
        )
        if for_update:
            statement = statement.with_for_update()
        result = await db.execute(statement)
        return result.scalars().first()

    async def _get_owned_session(
        self, db: AsyncSession, user_id: int, public_id: str, *, for_update: bool = False
    ) -> MarketBacktestSession:
        session = await self._get_session_by_public_id(db, public_id, for_update=for_update)
        if session is None or session.user_id != user_id:
            raise BacktestPersistError(404, "回测场次不存在")
        return session

    async def _get_session_by_client_id(
        self, db: AsyncSession, user_id: int, client_session_id: str
    ) -> MarketBacktestSession | None:
        result = await db.execute(
            select(MarketBacktestSession).where(
                MarketBacktestSession.user_id == user_id,
                MarketBacktestSession.client_session_id == client_session_id,
                MarketBacktestSession.deleted_at.is_(None),
            )
        )
        return result.scalars().first()

    async def _get_trade_by_client_id(
        self, db: AsyncSession, session_id: int, client_trade_id: str
    ) -> MarketBacktestTrade | None:
        result = await db.execute(
            select(MarketBacktestTrade).where(
                MarketBacktestTrade.session_id == session_id,
                MarketBacktestTrade.client_trade_id == client_trade_id,
                MarketBacktestTrade.deleted_at.is_(None),
            )
        )
        return result.scalars().first()

    async def _require_open_trade(
        self, db: AsyncSession, session_id: int, client_trade_id: str
    ) -> MarketBacktestTrade:
        trade = await self._get_trade_by_client_id(db, session_id, client_trade_id)
        if trade is None:
            raise BacktestPersistError(404, "未找到对应持仓")
        if trade.status != "OPEN":
            raise BacktestPersistError(409, "持仓已平仓，无法改价")
        return trade

    async def _next_event_sequence(self, db: AsyncSession, session_id: int) -> int:
        result = await db.execute(
            select(func.max(MarketBacktestEvent.sequence_no)).where(
                MarketBacktestEvent.session_id == session_id,
                MarketBacktestEvent.deleted_at.is_(None),
            )
        )
        return int(result.scalar() or 0) + 1

    async def _next_trade_sequence(self, db: AsyncSession, session_id: int) -> int:
        result = await db.execute(
            select(func.max(MarketBacktestTrade.sequence_no)).where(
                MarketBacktestTrade.session_id == session_id,
                MarketBacktestTrade.deleted_at.is_(None),
            )
        )
        return int(result.scalar() or 0) + 1

    @staticmethod
    def _bump_cursor(
        session: MarketBacktestSession,
        bar_time: datetime,
        bar_index: int | None,
    ) -> None:
        if session.cursor_bar_time is None or bar_time >= session.cursor_bar_time:
            session.cursor_bar_time = bar_time
            if bar_index is not None:
                session.cursor_bar_index = bar_index

    @staticmethod
    def _parse_bar_time(value: datetime | int | float | str) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise BacktestPersistError(400, "K 线时间格式无效") from exc
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        raise BacktestPersistError(400, "K 线时间格式无效")

    @staticmethod
    def _normalize_side(value: str | None) -> str | None:
        if not value:
            return None
        return _SIDE_ALIASES.get(value.strip().upper())

    @staticmethod
    def _normalize_close_reason(value: str | None) -> str:
        if not value:
            return "MARKET_CLOSE"
        return _CLOSE_REASON_ALIASES.get(value.strip().upper(), "MARKET_CLOSE")

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    @staticmethod
    def _num(value) -> float | None:
        return float(value) if value is not None else None

    @classmethod
    def _serialize_session(cls, session: MarketBacktestSession) -> dict:
        return {
            "public_id": session.public_id,
            "client_session_id": session.client_session_id,
            "symbol": session.symbol,
            "interval": session.interval,
            "timeframe": session.timeframe,
            "status": session.status,
            "visibility": session.visibility,
            "start_bar_time": cls._iso(session.start_bar_time),
            "start_bar_index": session.start_bar_index,
            "initial_visible_bars": session.initial_visible_bars,
            "cursor_bar_time": cls._iso(session.cursor_bar_time),
            "cursor_bar_index": session.cursor_bar_index,
            "initial_balance": float(session.initial_balance or 0),
            "ending_balance": cls._num(session.ending_balance),
            "trade_count": session.trade_count,
            "closed_trade_count": session.closed_trade_count,
            "win_count": session.win_count,
            "realized_pnl": float(session.realized_pnl or 0),
            "created_at": cls._iso(session.created_at),
            "ended_at": cls._iso(session.ended_at),
        }

    @classmethod
    def _serialize_trade(cls, trade: MarketBacktestTrade) -> dict:
        return {
            "client_trade_id": trade.client_trade_id,
            "sequence_no": trade.sequence_no,
            "side": trade.side,
            "units": trade.units,
            "status": trade.status,
            "entry_price": cls._num(trade.entry_price),
            "entry_bar_time": cls._iso(trade.entry_bar_time),
            "entry_bar_index": trade.entry_bar_index,
            "sl_price": cls._num(trade.sl_price),
            "tp_price": cls._num(trade.tp_price),
            "close_price": cls._num(trade.close_price),
            "close_bar_time": cls._iso(trade.close_bar_time),
            "close_bar_index": trade.close_bar_index,
            "close_reason": trade.close_reason,
            "realized_pnl": cls._num(trade.realized_pnl),
        }

    @classmethod
    def _serialize_event(
        cls,
        event: MarketBacktestEvent,
        client_trade_id: str | None,
    ) -> dict:
        return {
            "sequence_no": event.sequence_no,
            "event_type": event.event_type,
            "bar_time": cls._iso(event.bar_time),
            "bar_index": event.bar_index,
            "client_trade_id": client_trade_id,
            "side": event.side,
            "units": event.units,
            "price": cls._num(event.price),
            "sl_price": cls._num(event.sl_price),
            "tp_price": cls._num(event.tp_price),
            "close_reason": event.close_reason,
        }

    @staticmethod
    def _serialize_event_result(
        session: MarketBacktestSession,
        trade: MarketBacktestTrade,
        event_type: str,
        sequence_no: int | None,
    ) -> dict:
        return {
            "public_id": session.public_id,
            "client_trade_id": trade.client_trade_id,
            "event_type": event_type,
            "sequence_no": sequence_no,
            "trade_status": trade.status,
        }


market_backtest_service = MarketBacktestService()

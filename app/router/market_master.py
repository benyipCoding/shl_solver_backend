from typing import Any, Awaitable

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients import db as db_client
from app.clients.db import get_db
from app.depends.jwt_guard import verify_superuser, verify_user
from app.models.user import User
from app.schemas.market_backtest import (
    BacktestEventCreate,
    BacktestSessionComplete,
    BacktestSessionCreate,
)
from app.schemas.response import APIResponse
from app.services.fxcm_market_sync import fxcm_market_sync_service
from app.services.fxcm_market_sync.types import PriorityForwardSyncError
from app.services.market_backtest import (
    BacktestPersistError,
    market_backtest_service,
)
from app.services.market_master import TwelveDataAPIError, market_master_service


router = APIRouter(
    prefix="/market_master",
    tags=["Market Master"],
    # dependencies=[Depends(verify_user)],  暂时注释掉 JWT 验证，后续根据需要开启
)


def _request_params(request: Request) -> dict[str, Any]:
    return dict(request.query_params)


async def _service_response(
    service_call: Awaitable[Any],
) -> APIResponse[Any] | JSONResponse:
    try:
        result = await service_call
    except TwelveDataAPIError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "message": exc.message,
                "data": exc.payload,
            },
        )

    return APIResponse(data=result)


@router.get(
    "/price",
    response_model=APIResponse[Any],
    summary="获取最新价格",
    description=(
        "使用 FXCM sidecar 返回最新价格。当前以 symbol 为主键做映射；"
        "对外保留兼容路径，便于前端无感切换。"
    ),
)
async def get_latest_price(
    request: Request,
    symbol: str | None = Query(
        None,
        description="标的代码。示例: AAPL、EUR/USD、BTC/USD。symbol/figi/isin/cusip 至少传一个。",
    ),
    figi: str | None = Query(
        None,
        description="FIGI 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    isin: str | None = Query(
        None,
        description="ISIN 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    cusip: str | None = Query(
        None,
        description="CUSIP 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    exchange: str | None = Query(None, description="交易所，可选。示例: NASDAQ。"),
    mic_code: str | None = Query(None, description="MIC 代码，可选。示例: XNAS。"),
    country: str | None = Query(
        None, description="国家名称或国家代码，可选。示例: US。"
    ),
    asset_type: str | None = Query(
        None,
        alias="type",
        description="资产类型，可选。示例: Common Stock、ETF、Digital Currency。",
    ),
    response_format: str | None = Query(
        None,
        alias="format",
        description="兼容保留参数。当前接口固定返回 JSON。",
    ),
    prepost: bool | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 聚合路径默认不区分盘前盘后。",
    ),
    dp: int | None = Query(
        None, ge=0, le=11, description="价格保留小数位，范围 0 到 11。"
    ),
):
    return await _service_response(
        market_master_service.get_latest_price(_request_params(request))
    )


@router.get(
    "/quote",
    response_model=APIResponse[Any],
    summary="获取实时报价快照",
    description=(
        "使用 FXCM sidecar 返回兼容报价快照，"
        "保留价格、开高低收、成交量、涨跌幅等核心字段。"
    ),
)
async def get_quote(
    request: Request,
    symbol: str | None = Query(
        None,
        description="标的代码。示例: AAPL、EUR/USD、BTC/USD。symbol/figi/isin/cusip 至少传一个。",
    ),
    figi: str | None = Query(
        None,
        description="FIGI 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    isin: str | None = Query(
        None,
        description="ISIN 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    cusip: str | None = Query(
        None,
        description="CUSIP 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    interval: str | None = Query(
        None,
        description="报价聚合周期，可选。常见值如 1min、5min、15min、30min、1day。",
    ),
    exchange: str | None = Query(None, description="交易所，可选。示例: NASDAQ。"),
    mic_code: str | None = Query(None, description="MIC 代码，可选。示例: XNAS。"),
    country: str | None = Query(
        None, description="国家名称或国家代码，可选。示例: US。"
    ),
    volume_time_period: int | None = Query(
        None,
        ge=1,
        description="平均成交量统计周期数，可选。",
    ),
    asset_type: str | None = Query(
        None,
        alias="type",
        description="资产类型，可选。示例: Common Stock、ETF、Digital Currency。",
    ),
    response_format: str | None = Query(
        None,
        alias="format",
        description="兼容保留参数。当前接口固定返回 JSON。",
    ),
    prepost: bool | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 聚合路径默认不区分盘前盘后。",
    ),
    eod: bool | None = Query(None, description="是否返回收盘日数据。"),
    rolling_period: int | None = Query(
        None,
        ge=1,
        le=168,
        description="滚动涨跌幅统计小时数，范围 1 到 168。",
    ),
    timezone: str | None = Query(
        None,
        description="输出时区，可选 Exchange、UTC 或具体 IANA 时区名。",
    ),
    dp: int | None = Query(
        None, ge=0, le=11, description="价格保留小数位，范围 0 到 11。"
    ),
):
    return await _service_response(
        market_master_service.get_quote(_request_params(request))
    )


@router.get(
    "/time-series",
    response_model=APIResponse[Any],
    summary="获取历史 K 线时序",
    description=(
        "通过 FXCM sidecar 返回兼容的历史 OHLCV 时序数据。"
        "服务端会补齐 outputsize、周期映射、时区处理与部分聚合逻辑。"
        "每次返回当前窗口以及该品种/周期的 K 线总数 total、窗口起点 offset。"
        "可通过 offset / around_time 按需截取窗口，而不必一次拉全量历史。"
    ),
)
async def get_time_series(
    request: Request,
    interval: str = Query(
        ...,
        description=(
            "时间粒度。当前兼容层支持 1min、5min、15min、30min、45min、1h、2h、4h、8h、1day、1week、1month。"
        ),
    ),
    symbol: str | None = Query(
        None,
        description="标的代码。示例: AAPL、EUR/USD、BTC/USD。symbol/figi/isin/cusip 至少传一个。",
    ),
    figi: str | None = Query(
        None,
        description="FIGI 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    isin: str | None = Query(
        None,
        description="ISIN 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    cusip: str | None = Query(
        None,
        description="CUSIP 标识，可选。为兼容旧入参保留；当前 FXCM 接入主要按 symbol 映射。",
    ),
    outputsize: int | None = Query(
        None,
        ge=1,
        le=5000,
        description="返回数据点数量。当前兼容层支持范围 1 到 5000。",
    ),
    exchange: str | None = Query(None, description="交易所，可选。示例: NASDAQ。"),
    mic_code: str | None = Query(None, description="MIC 代码，可选。示例: XNAS。"),
    country: str | None = Query(
        None, description="国家名称或国家代码，可选。示例: US。"
    ),
    asset_type: str | None = Query(
        None,
        alias="type",
        description="资产类型，可选。示例: Common Stock、ETF、Digital Currency。",
    ),
    timezone: str | None = Query(
        None,
        description="输出时区，可选 Exchange、UTC 或具体 IANA 时区名。",
    ),
    start_date: str | None = Query(
        None,
        description="开始时间，可传 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS。",
    ),
    end_date: str | None = Query(
        None,
        description="结束时间，可传 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS。",
    ),
    offset: int | None = Query(
        None,
        ge=0,
        description="从最早一根 K 线起的 0-based 偏移。传入后按时间升序返回该窗口，适合回测跳转与向未来续载。",
    ),
    around_time: str | None = Query(
        None,
        description="定位到该时间附近的 K 线窗口。可传 ISO 时间或 unix 秒。常用于还原回测起点。",
    ),
    before_count: int | None = Query(
        None,
        ge=0,
        le=5000,
        description="配合 around_time：在定位 K 线之前额外保留多少根上下文，默认 0。",
    ),
    after_date: str | None = Query(
        None,
        description="返回该时间之后的 K 线（不含该时刻本身），用于向未来续载。",
    ),
    date: str | None = Query(
        None,
        description="指定单日数据，可传具体日期，或 today、yesterday。",
    ),
    order: str | None = Query(
        None,
        description="返回排序方向。当前兼容层常用值为 asc 或 desc。",
    ),
    prepost: bool | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 聚合路径默认不区分盘前盘后。",
    ),
    response_format: str | None = Query(
        None,
        alias="format",
        description="兼容保留参数。当前接口固定返回 JSON。",
    ),
    adjust: str | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 历史接口未使用该参数。",
    ),
    previous_close: bool | None = Query(
        None,
        description="是否在结果中补充上一根 K 线的收盘价。",
    ),
    dp: int | None = Query(
        None, ge=0, le=11, description="价格保留小数位，范围 0 到 11。"
    ),
    filter_non_trading: bool = Query(
        False,
        description=(
            "是否过滤明显处于休市状态的平盘 K 线。FXCM 默认可直接使用原始时序；"
            "仅在你确认某些标的存在休市占位 K 线时再设为 true。"
        ),
    ),
):
    return await _service_response(
        market_master_service.get_time_series(
            {
                key: value
                for key, value in _request_params(request).items()
                if key != "filter_non_trading"
            },
            filter_non_trading=filter_non_trading,
        )
    )


@router.get(
    "/symbol-search",
    response_model=APIResponse[Any],
    summary="搜索交易标的",
    description=(
        "使用 FXCM offers 列表并补充手工映射，适合前端做股票、外汇、指数、"
        "加密货币与贵金属等交易标的搜索联想。"
    ),
)
async def symbol_search(
    request: Request,
    symbol: str = Query(
        ...,
        min_length=1,
        description="搜索关键字，可传代码、简称或常见别名。",
    ),
    outputsize: int | None = Query(
        None,
        ge=1,
        le=120,
        description="最多返回多少条匹配结果。当前兼容层上限为 120。",
    ),
    show_plan: bool | None = Query(
        None,
        description="是否返回供应商可用性字段，便于前端提示该标的是否有限制。",
    ),
):
    return await _service_response(
        market_master_service.search_symbols(_request_params(request))
    )


@router.get(
    "/sync/status",
    response_model=APIResponse[Any],
    summary="获取当前同步状态和运行中的任务",
)
async def get_sync_status():
    async with db_client.async_session() as db:
        status_info = await fxcm_market_sync_service.get_status(db)
        running_tasks = await fxcm_market_sync_service.get_running_tasks(db)
        sync_states = await fxcm_market_sync_service.get_sync_states(db)
        return APIResponse(
            data={
                "status": status_info,
                "running_tasks": running_tasks,
                "states": sync_states["items"],
                "rotation": sync_states["rotation"],
            }
        )


@router.post(
    "/sync/latest",
    response_model=APIResponse[Any],
    summary="优先同步当前品种当前周期到最新日期",
    description=(
        "仅超级管理员可用。将指定交易品种、周期插入最高优先级队列，"
        "只从本地已有的最新 K 线向前追赶到当前日期，不回补更早的历史缺口。"
        "该任务优先于后台定时抓取。"
    ),
    dependencies=[Depends(verify_superuser)],
)
async def sync_latest_bars(
    symbol: str = Query(..., min_length=1, description="交易品种。示例: GBP/USD。"),
    interval: str = Query(
        ...,
        min_length=1,
        description="K 线周期。示例: 1day、D1、1h、H1。",
    ),
):
    try:
        async with db_client.async_session() as db:
            result = await fxcm_market_sync_service.request_priority_forward_sync(
                db,
                symbol=symbol,
                interval=interval,
            )
    except PriorityForwardSyncError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "message": exc.message,
                "data": None,
            },
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "code": 500,
                "message": f"{type(exc).__name__}: {exc}",
                "data": None,
            },
        )

    return APIResponse(data=result)


@router.post(
    "/sync/repair",
    response_model=APIResponse[Any],
    summary="优先重采框选时间段的 K 线并覆盖差异",
    description=(
        "仅超级管理员可用。按指定交易品种、周期和时间段立即向 FXCM sidecar 重新采集，"
        "该任务插入最高优先级队列，无需等待后台定时任务完成。"
        "若新数据与本地存在差异（缺漏、OHLCV 不同、多余脏数据），以新数据为准。"
    ),
    dependencies=[Depends(verify_superuser)],
)
async def repair_bars(
    symbol: str = Query(..., min_length=1, description="交易品种。示例: XAU/USD。"),
    interval: str = Query(
        ...,
        min_length=1,
        description="K 线周期。示例: 5min、M5、1h、H1。",
    ),
    start_date: str = Query(
        ...,
        min_length=1,
        description="框选开始时间，ISO 日期或日期时间。示例: 2026-07-01T00:00:00Z。",
    ),
    end_date: str = Query(
        ...,
        min_length=1,
        description="框选结束时间，ISO 日期或日期时间。示例: 2026-08-31T23:59:59Z。",
    ),
):
    try:
        async with db_client.async_session() as db:
            result = await fxcm_market_sync_service.request_priority_range_repair(
                db,
                symbol=symbol,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
            )
    except PriorityForwardSyncError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "message": exc.message,
                "data": None,
            },
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "code": 500,
                "message": f"{type(exc).__name__}: {exc}",
                "data": None,
            },
        )

    return APIResponse(data=result)


@router.get(
    "/market-movers/{market}",
    response_model=APIResponse[Any],
    summary="获取市场异动榜",
    description=(
        "使用 FXCM sidecar 的市场候选列表与批量 quote 做涨幅榜或跌幅榜聚合。"
        "FXCM 无覆盖的分类会返回空列表，而不会再回落到 Yahoo。"
    ),
)
async def get_market_movers(
    request: Request,
    market: str = Path(
        ...,
        description="市场类型。当前兼容层支持 stocks、etf、mutual_funds、forex、crypto。",
    ),
    direction: str | None = Query(
        None,
        description="榜单方向。当前兼容层支持 gainers 或 losers。",
    ),
    outputsize: int | None = Query(
        None,
        ge=1,
        le=50,
        description="榜单返回条数。当前兼容层支持范围 1 到 50。",
    ),
    country: str | None = Query(
        None,
        description="国家过滤，仅非货币类市场适用。可传国家名称或国家代码。",
    ),
    price_greater_than: str | None = Query(
        None,
        description="仅返回价格高于该阈值的标的。",
    ),
    dp: int | None = Query(
        None, ge=0, le=11, description="价格保留小数位，范围 0 到 11。"
    ),
):
    return await _service_response(
        market_master_service.get_market_movers(market, _request_params(request))
    )


@router.get(
    "/watchlist/quotes",
    response_model=APIResponse[Any],
    summary="批量获取自选列表报价",
    description=(
        "面向前端自选页的聚合接口。传入逗号分隔的 symbols 后，后端会通过 FXCM sidecar 的单会话批量 quote 接口，"
        "统一返回归一化后的价格结构，并保留部分失败项。单次最多 10 个代码。"
    ),
)
async def get_watchlist_quotes(
    symbols: str = Query(
        ...,
        description="自选代码列表，使用逗号分隔。示例: AAPL,MSFT,NVDA。单次最多 10 个。",
    ),
    interval: str | None = Query(
        None,
        description="可选 quote 聚合周期。常见值如 1min、5min、15min、30min、1day。",
    ),
    exchange: str | None = Query(None, description="交易所过滤，可选。示例: NASDAQ。"),
    mic_code: str | None = Query(None, description="MIC 代码过滤，可选。示例: XNAS。"),
    country: str | None = Query(
        None, description="国家名称或国家代码，可选。示例: US。"
    ),
    asset_type: str | None = Query(
        None,
        alias="type",
        description="资产类型过滤，可选。示例: Common Stock、ETF、Digital Currency。",
    ),
    timezone: str | None = Query(
        None,
        description="输出时区，可选 Exchange、UTC 或具体 IANA 时区名。",
    ),
    eod: bool | None = Query(None, description="是否返回收盘日数据。"),
    prepost: bool | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 聚合路径默认不区分盘前盘后。",
    ),
    dp: int | None = Query(
        None, ge=0, le=11, description="价格保留小数位，范围 0 到 11。"
    ),
):
    return await _service_response(
        market_master_service.get_watchlist_quotes(
            [item for item in symbols.split(",")],
            interval=interval,
            exchange=exchange,
            mic_code=mic_code,
            country=country,
            asset_type=asset_type,
            timezone=timezone,
            eod=eod,
            prepost=prepost,
            dp=dp,
        )
    )


@router.get(
    "/kline/defaults",
    response_model=APIResponse[Any],
    summary="获取前端友好的 K 线默认结构",
    description=(
        "对 FXCM sidecar 历史 K 线接口做前端友好的默认参数封装。默认 outputsize=120、timezone=Exchange、"
        "order=desc、previous_close=true，默认不过滤休市时段，并补齐 filtering 信息与 candles 数组结构。"
        "响应包含 total（该品种/周期全部 K 线数量）与 offset（当前窗口在全量中的起点）。"
    ),
)
async def get_kline_defaults(
    symbol: str = Query(..., description="标的代码。示例: AAPL、EUR/USD、BTC/USD。"),
    interval: str = Query(
        "1day",
        description="K 线粒度，默认 1day。常见值如 1min、5min、1h、1day、1week。",
    ),
    outputsize: int = Query(
        120,
        ge=1,
        le=5000,
        description="默认返回 120 根 K 线，可按需调小或调大。",
    ),
    exchange: str | None = Query(None, description="交易所过滤，可选。示例: NASDAQ。"),
    mic_code: str | None = Query(None, description="MIC 代码过滤，可选。示例: XNAS。"),
    country: str | None = Query(
        None, description="国家名称或国家代码，可选。示例: US。"
    ),
    asset_type: str | None = Query(
        None,
        alias="type",
        description="资产类型过滤，可选。示例: Common Stock、ETF、Digital Currency。",
    ),
    timezone: str = Query(
        "Exchange",
        description="输出时区，默认 Exchange。也可传 UTC 或具体 IANA 时区名。",
    ),
    start_date: str | None = Query(
        None,
        description="开始时间，可传 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS。",
    ),
    end_date: str | None = Query(
        None,
        description="结束时间，可传 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS。",
    ),
    offset: int | None = Query(
        None,
        ge=0,
        description="从最早一根 K 线起的 0-based 偏移。传入后按时间升序返回该窗口。",
    ),
    around_time: str | None = Query(
        None,
        description="定位到该时间附近的 K 线窗口。可传 ISO 时间或 unix 秒。",
    ),
    before_count: int | None = Query(
        None,
        ge=0,
        le=5000,
        description="配合 around_time：在定位 K 线之前额外保留多少根上下文，默认 0。",
    ),
    after_date: str | None = Query(
        None,
        description="返回该时间之后的 K 线（不含该时刻本身），用于向未来续载。",
    ),
    adjust: str | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 历史接口未使用该参数。",
    ),
    prepost: bool | None = Query(
        None,
        description="兼容保留参数。当前 FXCM 聚合路径默认不区分盘前盘后。",
    ),
    dp: int | None = Query(
        None, ge=0, le=11, description="价格保留小数位，范围 0 到 11。"
    ),
    filter_non_trading: bool = Query(
        False,
        description=(
            "是否过滤明显处于休市状态的平盘 K 线。FXCM 默认可直接给图表渲染；"
            "仅在你确认某些标的存在休市占位 K 线时再设为 true。"
        ),
    ),
):
    return await _service_response(
        market_master_service.get_kline_defaults(
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            exchange=exchange,
            mic_code=mic_code,
            country=country,
            asset_type=asset_type,
            timezone=timezone,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            around_time=around_time,
            before_count=before_count,
            after_date=after_date,
            adjust=adjust,
            prepost=prepost,
            dp=dp,
            filter_non_trading=filter_non_trading,
        )
    )


@router.get(
    "/search/markets",
    response_model=APIResponse[Any],
    summary="获取可搜索的热门品类",
    description=(
        "从 MarketInstrument 表聚合当前可搜索、已启用标的的 distinct market 字段，"
        "供前端热门品类快捷入口使用。"
    ),
)
async def list_search_markets():
    return await _service_response(market_master_service.list_search_markets())


@router.get(
    "/search/unified",
    response_model=APIResponse[Any],
    summary="统一市场搜索结构",
    description=(
        "对 FXCM offers 搜索结果做前端友好归一化，统一返回 symbol、label、market、"
        "asset_type、country、currency 等固定字段，并补上关键标的的手工映射。"
        "支持按 keyword 搜索，或按 market（MarketInstrument.market）浏览品类。"
    ),
)
async def get_unified_search(
    keyword: str | None = Query(
        None,
        description="搜索关键词，可传代码、简称或公司名片段。与 market 至少传一个。",
    ),
    market: str | None = Query(
        None,
        description=(
            "按 MarketInstrument.market 过滤/浏览品类。"
            "取值来自 /search/markets，例如 Forex、Crypto、Commodities。"
        ),
    ),
    outputsize: int = Query(
        10,
        ge=1,
        le=30,
        description="归一化结果条数，默认 10，建议前端联想不超过 10。",
    ),
    show_plan: bool = Query(
        False,
        description="是否透出供应商可用性字段，便于前端提示标的可用性。",
    ),
):
    return await _service_response(
        market_master_service.search_unified(
            keyword=keyword,
            market=market,
            outputsize=outputsize,
            show_plan=show_plan,
        )
    )


def _backtest_error_response(exc: BacktestPersistError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.status_code,
            "message": exc.message,
            "data": None,
        },
    )


@router.post(
    "/backtest/sessions",
    response_model=APIResponse[Any],
    summary="创建逐K回测场次",
    description="用户开启逐K回测时创建一场 session。相同 client_session_id 重复提交会返回已有场次。",
)
async def create_backtest_session(
    payload: BacktestSessionCreate,
    user: User = Depends(verify_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await market_backtest_service.create_session(db, user, payload)
    except BacktestPersistError as exc:
        return _backtest_error_response(exc)
    return APIResponse(data=result)


@router.get(
    "/backtest/sessions",
    response_model=APIResponse[Any],
    summary="回测场次列表",
    description="按创建时间倒序列出当前用户的逐K回测记录，用于历史弹窗还原播放。",
)
async def list_backtest_sessions(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    size: int = Query(20, ge=1, le=100, description="每页条数"),
    user: User = Depends(verify_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await market_backtest_service.list_sessions(db, user, page, size)
    except BacktestPersistError as exc:
        return _backtest_error_response(exc)
    return APIResponse(data=result)


@router.get(
    "/backtest/sessions/{public_id}",
    response_model=APIResponse[Any],
    summary="回测场次详情",
    description="返回场次、成交和事件流，供前端按 sequence_no 一比一还原播放。",
)
async def get_backtest_session(
    public_id: str = Path(..., min_length=1, description="回测场次 public_id"),
    user: User = Depends(verify_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await market_backtest_service.get_session_detail(
            db, user, public_id
        )
    except BacktestPersistError as exc:
        return _backtest_error_response(exc)
    return APIResponse(data=result)


@router.post(
    "/backtest/sessions/{public_id}/events",
    response_model=APIResponse[Any],
    summary="写入回测开仓/改价/平仓事件",
)
async def record_backtest_event(
    payload: BacktestEventCreate,
    public_id: str = Path(..., min_length=1, description="回测场次 public_id"),
    user: User = Depends(verify_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await market_backtest_service.record_event(
            db, user, public_id, payload
        )
    except BacktestPersistError as exc:
        return _backtest_error_response(exc)
    return APIResponse(data=result)


@router.post(
    "/backtest/sessions/{public_id}/complete",
    response_model=APIResponse[Any],
    summary="结束逐K回测场次",
    description="退出回测时调用。仍未平仓的持仓会按 mark_price 强制平仓；若本场无任何开仓则软删除，不进入历史列表。",
)
async def complete_backtest_session(
    payload: BacktestSessionComplete,
    public_id: str = Path(..., min_length=1, description="回测场次 public_id"),
    user: User = Depends(verify_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await market_backtest_service.complete_session(
            db, user, public_id, payload
        )
    except BacktestPersistError as exc:
        return _backtest_error_response(exc)
    return APIResponse(data=result)

# 福汇历史数据与本地实时模式

实时交易使用 ForexConnect Demo 账户执行。交易行为、费用口径及启用方式见 [本地 Demo 交易](fxcm-demo-trading.md)；历史回测保持原有模拟行为。

## 1. 现有链路与关键发现

当前架构是 Python 3.7 的 `fxcm-api` 包装 ForexConnect 1.6.43，Python 3.11 的业务后端通过 HTTP 使用它。两套运行环境继续保持独立。

历史采集路径：

```text
lifespan → FXCMMarketSyncScheduler → FXCMMarketSyncService
  → instrument_sync / state_sync / bar_sync
  → FXCMSidecarService.get_history → fxcm-api /history
  → ForexConnect.get_history → MarketOHLCVBar / MarketBarSyncState
```

调度器依据 `APP_ENV` 和 `FXCM_SYNC_ENABLED` 启动，按状态安排增量采集、重叠回补及历史回填。`market_instrument` 保存品种资料，`market_bar_sync_state` 保存采集进度；K 线按品种、周期、价格类型和时间唯一定位，写入使用 upsert。手动追赶和区间修复也通过这套服务工作；生产复制还使用 repair outbox。

前端读取路径：

```text
MarketMasterPage → fetchKlinePage → Next.js /api/market_master/*
  → /api_v1/market_master/kline/defaults 或 time-series
  → MarketMasterService → 本地 PostgreSQL → 格式转换 / 过滤 / 分页
```

前端已经围绕同一组 K 线维护指标、画线、分笔、线段、支撑阻力、模拟订单和逐 K 回测。数据库总数、offset、around_time、before_count 和 after_date 共同支撑分页及回放。因此实时模式复用此页面和历史分页，只在最新端叠加实时快照和更新。逐 K 回测和回放仅在历史模式提供；切回历史模式时重新读取数据库。

发现并修正的连接问题：

- 已安装的 SDK 没有 `ForexConnect.is_connected()`。原检查捕获 `AttributeError` 后总是重新登录，实际上无法稳定复用连接。现在使用 `fx.session.session_status == CONNECTED`。
- SDK 的登录等待状态回调，原断线回调又获取登录锁，存在互相等待风险。回调现在只标记连接代次和重建需求，重建放回请求线程完成。
- SDK 在会话中复用历史价格 communicator，新增互斥锁串行访问，防止实时初始化快照与历史定时采集并发操作它。

上述修正保持历史 HTTP 响应格式不变。新增实时服务没有数据库写入、迁移或调度任务。

## 2. 官方用法与方案选择

查阅日期：2026-10-13。已直接读取以下官方仓库及其链接的 SDK 示例，并核对本机安装包源码：

- [FXCM ForexConnectAPI 官方说明](https://github.com/fxcm/ForexConnectAPI)：SDK 支持历史价格和最新价格订阅；Table Manager 的表更新事件可以用于实时行情。
- [官方说明链接的 BuildLiveHistory.py](https://github.com/gehtsoft/forex-connect/blob/master/samples/Python/BuildLiveHistory.py)：`Common.subscribe_table_updates(fx.get_table(ForexConnect.OFFERS), on_change_callback=...)`，先订阅再加载历史，避免初始化窗口丢行情。
- [SetSubscriptionStatus.py](https://github.com/gehtsoft/forex-connect/blob/master/samples/Python/SetSubscriptionStatus.py)：通过 `SET_SUBSCRIPTION_STATUS` 和 offer ID 将订阅状态设置为 `T`。
- [FXCM 官方入门文档](https://apiwiki.fxcorporate.com/api/Getting%20Started.pdf)：官方入口参考；此次实现主要依据上述实际读取的 Python 示例及 SDK 源码。

沿用当前 ForexConnect 账号、登录流程和协议，无需换成 REST Token 方案。供应商端的实时订阅和浏览器传输是两层：本实现使用 ForexConnect 原生回调接收价格，以 SSE 向浏览器单向推送。SSE 能直接通过现有 Next.js Route Handler 流式代理，并保留同源 Cookie；当前没有必须使用双向 WebSocket 的数据需求。

交易指令通过独立的 `/trading` 链路发送给 ForexConnect，并从账户表确认结果；行情 SSE 不负责执行订单。

## 3. 新增的数据流与生命周期

```text
FXCM Offers 表更新
  → fxcm-api Feed：线程安全报价缓冲 / K 线合成
  → RealtimeHub：同品种同周期共享订阅
  → /realtime/stream (SSE)
  → 后端 /api_v1/market_master/realtime/stream
  → Next.js /api/market_master/realtime/stream
  → useRealtimeMarket → 原 MarketMasterPage
```

首次订阅先注册回调，再获取最近 256 根历史 K 线作为快速校准快照，合并初始化期间到达的报价。前端已有的数据库历史窗口保持不变；若两个窗口没有重叠，自动请求一次 `snapshot_size=5000` 的扩大快照，仍有缺口时明确停止，不拼接不连续行情。两层 Python 接口都将快照数量限制在 2–5,000，默认 256。SDK 回调只复制报价到有界缓冲，不操作 asyncio 对象、数据库或图表。

切换实时模式、品种或周期时，数据库历史加载与 SSE 连接并行。历史未加载完成时，前端暂存当前品种/周期的事件，加载完成后按快照、增量顺序应用；缓冲最多 128 个事件，溢出后重新获取快照。切换时丢弃旧图表缓冲并取消旧的历史 HTTP 请求。加载更早历史只暂停图表应用，不关闭实时连接。

每 500ms 批量发送缓冲中的报价及受影响 K 线，同时间戳的 K 线覆盖、跨周期的 K 线追加；批次保留报价序列。每 10 秒发送心跳，区分传输中断与“连接正常但暂无新报价”。止盈止损由福汇服务器执行。

每个客户端队列最多 32 个事件，每个上游缓冲最多 20,000 条报价，每进程最多 16 个品种/周期/快照规模组合。溢出时明确终止并要求重新取快照，避免静默丢数据。浏览器按 1、2、4、8、16、30 秒退避重连；切换品种、周期、模式以及组件卸载均关闭原浏览器连接。

同一品种的不同周期共享报价订阅引用计数。如果原来未订阅则自动设置为 `T`，最后一个使用者退出时尝试恢复原状态。若福汇已经断线，恢复只能尽力执行；可在 Trading Station 核对品种订阅状态。原本已订阅的品种保持订阅。

最后一个实时消费者退出后，服务端将最近最多 4 个空闲订阅保留 30 秒，期间只接收报价，不触发历史校准查询。再次访问直接返回最新内存快照；若空闲期间跨过 K 线边界，则在恢复访问后校准收盘数据。超时、超过缓存上限或服务退出时取消任务并恢复订阅；SDK 会话已失效的缓存不可复用。历史查询继续复用现有会话，实时退出不会注销历史会话。

账户 Bid/Ask 和图表 K 线是独立连接，所以账户价格先跳动不代表 K 线快照已经完成。状态栏分别显示“正在连接 K 线行情”“行情已连接，正在校准 K 线”“行情已连接，正在加载图表历史”，只有快照成功应用且报价有效才显示“实时更新中”。首次访问未缓存的品种/周期仍受福汇历史接口和网络延迟影响。

## 4. K 线口径与前端功能

价格沿用原 `mid` 口径：分别更新 Bid/Ask OHLC，再取两侧对应字段的平均值。开盘沿用 `get_history()` 默认的 `PREVIOUS_CLOSE`。不凭空补出休市期间的空 K 线。

周期覆盖现有 sidecar 的全部周期：1min、5min、15min、30min、45min、1h、2h、4h、8h、1day、1week、1month。其中 45min、2h、4h、8h 保持历史服务已有的 UTC 聚合边界。

实际账户查询得到：日线从纽约 17:00 开始，样本为 21:00 UTC；周线示例为周六 21:00 UTC；月线示例为前一月最后一天 21:00 UTC。日/周/月根据供应商快照的锚点和纽约日历处理夏令时及月长，不能直接对 Unix 时间取固定秒数余数。对于非纽约 17:00 的原生锚点保留其 UTC 日历锚点。后端将这些周期的显示日期转换为历史接口相同的 date-only 标识，避免一根日线被前端识别为两根。

| 功能 | 实时模式行为 |
| --- | --- |
| 品种搜索、收藏、周期切换 | 复用原组件；切换时释放旧订阅 |
| 历史拖动、分页、定位 | 复用数据库分页；更新时保持视口，位于最新端时跟随新 K 线 |
| EMA、MACD、BOLL、成交量 | 复用计算和绘制；正在形成的 K 线量暂留空，收盘后从供应商历史校准 |
| 手动画线、吸附、价格轴、布局 | 复用原组件和状态 |
| 自动分笔、线段、支撑阻力 | 通过原图表数据更新和分析入口刷新 |
| 下单、盈亏、止盈止损、部分平仓、订单管理 | 使用 ForexConnect Demo 账户执行；Bid/Ask、成交、费用、账户余额以福汇回报为准 |
| 逐 K 回测、自动策略、回测记录、分享与回放 | 实时模式不提供这些入口；切回历史数据后使用，保留原有行为 |
| 手动同步最新、框选修复 | 实时模式隐藏；历史模式保留原管理接口 |

实时持仓由福汇管理，切换模式或关闭页面不会自动平仓，也不会写入回测场次。原回测记录及分享继续使用现有持久化。实时模式隐藏桌面端和移动端的逐 K 回测、回测记录、播放及步进控件，并停止分享回放处理；异步回放请求也不能在切换后进入实时模式。自动策略仍按原功能在历史模式的逐 K 回测中执行，没有启用自动实时交易。

连接超过 30 秒没有新报价时显示等待/可能休市，过期报价不可用于新开仓。重连时校准 K 线，成交另从福汇账户表恢复。若断线/历史落后超过 5,000 根导致两个图表窗口没有重叠，显示缺口；可切回历史模式“同步最新”，防止拼接不连续行情。

## 5. 仅本地启用

在 `fxcm-api/.env` 和 `shl-solver-backend/.env` 都添加：

```dotenv
APP_ENV=local
FXCM_REALTIME_ENABLED=true
```

业务后端还应指向本地 sidecar（已有默认值）：

```dotenv
FXCM_API_BASE_URL=http://127.0.0.1:8100
```

sidecar 保留既有 USERNAME、PASSWORD、FXCM_URL 等配置。前端保留既有 `API_BASE_URL` 或 `NEXT_PUBLIC_BASE_URL`，通常为 `http://127.0.0.1:8000/api_v1`。福汇账号和密码不传给浏览器。

按原方式启动业务后端、数据库及其他依赖。sidecar 建议使用单 worker 和回环地址：

```powershell
# fxcm-api 目录，Python 3.7
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8100 --reload
```

```powershell
# shl-solver-frontend 目录
npm run dev
```

`APP_ENV` 已为 `local`、`dev` 或 `development` 的服务无需修改此值。修改环境变量后重启两个 Python 服务并刷新页面。打开 `/market-master`，页面最上方“行情模式”选择“实时数据（本地）”。开发环境始终显示模式栏；未启用或检测失败时会显示原因，实时选项不可选，服务启动后可点击“重新检测”。仍默认进入历史模式，实时模式不写入浏览器持久偏好。

三层保护：sidecar 和业务后端均要求显式环境 allow-list（local/dev/development）加功能开关；前端代理还要求 `NODE_ENV=development`。`prod`、`production`、`staging`、未知环境均禁止实时接口，即使功能开关误设为 true。生产构建不显示切换入口，能力接口在前端直接返回 disabled，流接口直接返回 404。

此保护覆盖新增实时链路。历史服务原有的“本地无数据时按需调用 sidecar”逻辑仍然存在，生产侧应继续保证历史库完整。

## 6. 验证与边界

离线测试命令：

```powershell
# fxcm-api
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
# shl-solver-backend
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
# shl-solver-frontend
node --test components/market-master/*.test.mjs
npx tsc --noEmit --pretty false
npm run build
```

新增用例覆盖同 K 更新、跨 K、乱序、重复、夏令时、周/月边界、生产关闭、共享订阅、慢消费者、断线清理、重连合并、同秒报价、模拟成交和前端取消/重试。原回测、交易策略和图表纯逻辑用例也参与回归。需要独立 PostgreSQL 测试 URL 的集成用例在未配置时跳过，不使用业务数据库代替。

含 Demo 交易接入的回归结果：sidecar 22 项通过；业务后端 29 项通过、7 项隔离数据库测试跳过；前端 204 项通过、1 项可选性能基准跳过。TypeScript 检查、新增 TypeScript 模块 ESLint 检查和 Next.js 生产构建通过。

真实账户行情联调获得 5,000 根 EUR/USD 初始化 K 线和连续报价；同一会话中执行历史查询成功；真实 HTTP SSE 收到 snapshot 和 candles；断开客户端后频道数归零、测试服务正常退出。Demo 订单执行的单独验证见交易指南。

补充浏览器联调：通过本地 3000 → 8000 → 8100 完整链路，在 XAU/USD 页面切换实时模式，收到 5,000 根快照与连续 candles 报价，显示“实时更新中”。桌面和移动视口均无回测入口，切回历史模式恢复入口并关闭 EventSource，页面无运行时异常；另验证了未启用提示和“重新检测”后恢复可选。渲染回归覆盖实时模式隐藏回测控件和生产模式隐藏行情切换栏。

2026-10-14 切换性能优化后，新增回归覆盖并行连接/历史加载、分页期间保留订阅、跨图表缓冲隔离、快照缺口扩大、缓冲上限、空闲订阅复用/淘汰/退出清理，以及恢复访问时的收盘校准。真实行情浏览器采样中，XAU/USD 切到 H4 约 1.54 秒、回到 D1 约 0.91 秒、再次回到 H4 约 0.78 秒；切换 EUR/USD 和返回 XAU/USD 分别约 0.94 和 0.90 秒，无页面运行时错误。这些是当次本地链路从切换到显示“实时更新中”的观测值，首次登录、网络和福汇服务延迟仍会影响耗时。

仍需按实际使用品种验收完整交易交互、长时间运行、网络中断及跨交易日表现。本地单进程内存共享是有意的部署边界；若以后需要多进程或生产实时服务，应再引入独立行情进程/消息总线、持久化与相应鉴权，而不是直接打开当前开发开关。

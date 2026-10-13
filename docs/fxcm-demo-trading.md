# 本地 ForexConnect Demo 交易

## 官方接口与实际核对

核对日期：2026-10-13。现有 Python 3.7 / ForexConnect 1.6.43 SDK 支持交易，无需更换成 REST 或 WebSocket SDK。依据已读取的官方仓库示例：

- [FXCM ForexConnectAPI](https://github.com/fxcm/ForexConnectAPI)
- [OpenPosition.py](https://github.com/gehtsoft/forex-connect/blob/master/samples/Python/OpenPosition.py)：`TRUE_MARKET_OPEN`，按账户与品种查询 `get_base_unit_size`。
- [ClosePosition.py](https://github.com/gehtsoft/forex-connect/blob/master/samples/Python/ClosePosition.py)：`TRUE_MARKET_CLOSE`，指定 TradeID、账户、数量以及与持仓相反的方向。
- [AttachStopLimitOrder.py](https://github.com/gehtsoft/forex-connect/blob/master/samples/Python/AttachStopLimitOrder.py)：创建或编辑服务器端 Stop / Limit，支持关联 TradeID。
- [RemoveOrder.py](https://github.com/gehtsoft/forex-connect/blob/master/samples/Python/RemoveOrder.py)：撤销指定 OrderID。

本机 SDK 还提供 `commissions_provider`、`rollover_provider`、`trading_settings_provider`，以及 Accounts / Trades / ClosedTrades / Orders 表。接口响应代表请求处理结果，最终成交必须由持仓/已平仓表确认。

## 行为

历史模式仍使用原本的本地回测与记录保存。实时模式使用福汇 Demo 账户，所有开仓、平仓、部分平仓、止损/止盈新增、修改及撤销均发送给 ForexConnect；页面不再用 mid K 线合成本地成交。

买入显示 Ask，卖出显示 Bid。服务器市价单的成交回报才是最终价格；请求采用 FOK，不自动拆单或重发。开仓止损/止盈通过同一开仓请求的 RATE_STOP / RATE_LIMIT 提交。数量根据福汇返回的最小、最大数量和基础单位校验，价格精度和止损/止盈方向、最小距离在 sidecar 校验，最终由福汇接受或拒绝。

余额、净值、保证金和毛盈亏读取账户表。持仓及已平仓记录的毛盈亏、佣金、累计过夜费读取福汇表；净盈亏为 `gross_pl + rollover_interest - commission`，不使用 `价差 × 数量` 代替跨币种或 CFD 盈亏。佣金没有硬编码为 0；当前 Demo 实际返回 0。费用参考来自账户对应的 SDK provider，缺失值显示 `—`。过夜费会随结算日、持有时间和福汇规则改变，本次短时测试没有跨夜产生费用。

账户快照包含当前登录可见的全部持仓、委托和 ClosedTrades。下方交易表及图表按当前品种显示；终端同时列出账户全部持仓，其他品种可切换图表后管理。ClosedTrades 是 SDK 当前可见范围，不是完整历史账单，长期核账仍使用 Trading Station / 官方报表。

切换品种、周期、模式或关闭浏览器不会自动平仓。福汇端止损/止盈继续有效。部分平仓后福汇可能给剩余持仓分配新 TradeID，页面依据下一次账户快照刷新，旧编号不会被当作剩余持仓继续操作。全部平仓需明确确认，且只处理确认列表中的当前品种持仓；任一步失败即停止并要求核对。

实时模式隐藏逐 K 回测、同步最新和修复 K 线；原历史模式入口保持不变。

## 链路与隔离

```text
浏览器交易终端
  → Next /api/market_master/trading/*（只在 development）
  → FastAPI /api_v1/market_master/trading/*（本地开关 + 管理员登录）
  → fxcm-api /trading/*（共享密钥 + 本地开关 + Demo 连接）
  → ForexConnect 市价/止损/止盈请求

福汇账户表推送 → SDK 内存表 → 每 750 ms SSE 快照 → 页面
```

750 ms 快照读取的是 SDK 内存表，不向福汇轮询 HTTP 接口。历史价格请求有自己的 `_history_request_lock`；交易串行锁独立于该锁，正常已连接时不等待历史请求完成。此次没有停用或修改历史采集定时任务。将来如观察到账号级限流，应基于实际延迟/拒单数据再调整调度。

共享 Broker 账户只向已登录的管理员开放，普通登录及游客不拥有交易权限。Next 写请求检查同源 Origin，sidecar 使用仅服务端持有的 bridge key。生产、未知环境、Real 连接均拒绝交易；本功能不能通过切换一项交易开关接入实盘。

## 幂等和断线

浏览器每次明确操作生成 UUID；sidecar 在 `.local/trading.sqlite3` 持久保存指令意图后才调用 SDK。同一 UUID 和相同内容不会再次发送；相同 UUID 不同内容返回 409。SQLite 文件被 Git 忽略，不应在存在待确认指令时删除。

成功请求还会等待账户表确认。明确服务器拒绝显示 rejected；超时或不明错误显示 unknown；已接收但尚未反映到账户表显示 pending。后两者不当作失败重试。页面保留待确认编号并提供“核对执行结果”，通过 TradeID、OrderID、CUSTOM_ID 或目标止损/止盈状态查询确认。存在待确认指令时禁止继续开仓，仍允许必要的持仓管理。

断线时停止接受新的页面操作，账户快照恢复后再启用。刷新页面从福汇账户恢复真实持仓，不会重放下单。进程意外终止后，已记录指令仍不会重复发送；若 SDK 当前可见表不足以证明执行结果，应先在 Trading Station 核对，而不是生成新的重复订单。

## 本地启用

本机已配置并热重载。两个 Python 服务均保留已有 realtime 配置，另需：

```dotenv
FXCM_TRADING_ENABLED=true
FXCM_TRADING_BRIDGE_KEY=两边相同的随机长密钥
```

`fxcm-api` 必须使用 `FXCM_CONNECTION=Demo`，`APP_ENV` 为 local/dev/development；业务后端同样要求这些本地环境之一及 realtime 开关。sidecar 使用一个 worker，绑定 127.0.0.1。前端 `npm run dev`，在 `/market-master` 使用管理员账户登录，选择“实时数据（本地）”。福汇终端显示 Demo 账户和账户币种后方可提交。

## 验证

- 离线测试覆盖生产及 Real 拒绝、bridge key、数量/价格规则、逐笔平仓方向、跨进程重启后的幂等、超时不重发、费用口径、管理员鉴权、同源写保护、前端重复点击及模式切换。
- 实际 Demo EUR/USD：买入、卖出、附带止损/止盈、修改止损、撤销及重新设置止盈、部分平仓、按新编号平掉剩余持仓、重复 UUID 不重复成交，均获账户表确认。测试结束持仓为 0。测试使用每次 2,000 Units，包含界面外 API 联调产生的少量 Demo 成交记录。
- 浏览器使用受控账户响应验证交易终端提交、全部平仓确认、按钮隐藏、历史模式回归及无回测保存请求；真实经纪商执行通过上述 sidecar 联调单独验证。
- 尚未实测跨交易日计息、长期网络故障和全部账户类型；费用始终显示福汇已回传的数据，不提前伪造结算。

# 本地 K 线修复同步到生产

## 首次启用

先在**本地源库**运行迁移，再重启执行本地修复的后端：

```powershell
python -m alembic upgrade head
```

迁移使用 `DATABASE_URL_SYNC`。后端的 `DATABASE_URL_ASYNC` 应指向同一个源库。
新表 `market_repair_outbox` 保存待同步任务和已完成记录，不参与行情复制。
生产端消费这些修复结果不依赖这张表；生产端若也运行本地修复服务，仍需按正常发布流程迁移。

保持原有 `.env` 配置：

```dotenv
DATABASE_URL_SYNC=postgresql+psycopg://.../local_database
MARKET_REPLICA_DATABASE_URL_SYNC=postgresql+psycopg://.../production_database
```

队列用于一个固定生产目标。请勿通过更换 `--target-url` 将同一队列轮流用于多个目标：
每条记录只有一个完成状态，不是多目标订阅队列。生产端需已有对应标的元数据；
首次使用先执行原有 `bootstrap` 或 `metadata` 流程。

## 日常使用

在页面中按原有方式修复本地 K 线后，只需执行：

```powershell
python scripts/sync_market_to_prod.py repair
```

- 每次成功修复都追加记录，包括 K 线未发生变化的成功修复。
- 修复 K 线与记录处于同一事务：全部成功或全部回滚。
- 命令读取开始时已经提交的未完成记录，每条本轮最多尝试一次；运行期间新提交的记录留到下次。
- 每条任务按本地最新数据覆盖生产对应的闭区间 `[start_at, end_at]`，并删除该范围内生产多余的 K 线。
- 生产提交成功后，才在本地记录 `synced_at`；已完成记录保留审计，下次自动跳过。
- 失败记录保留 `last_error`、`last_attempt_at` 和 `attempts`，不阻止其他记录；再次运行同一命令重试。
- 输出 `jobs_succeeded`、`jobs_failed`、`jobs_pending`；发生错误退出码为 1。

查看队列计数（`failed` 是 `pending` 的子集）：

```powershell
python scripts/sync_market_to_prod.py status
python scripts/sync_market_to_prod.py repair --json
```

查看失败详情可在本地源库查询：

```sql
SELECT id, symbol, interval, price_type, start_at, end_at,
       attempts, last_attempt_at, last_error
FROM market_repair_outbox
WHERE synced_at IS NULL
ORDER BY id;
```

原有带参数命令继续可用，用于补推**本功能启用前**的历史修复。历史修复无法自动追溯生成记录。
显式范围同步不会将队列记录标记完成；后续队列重复推送是幂等的。

```powershell
python scripts/sync_market_to_prod.py repair --symbol "XAU/USD" --interval 5min --start-date 2026-07-01 --end-date 2026-08-31
```

四个范围参数必须全部提供或全部省略。时间语义沿用旧命令：仅日期的 `2026-08-31`
表示该日 `00:00:00 UTC`，如需包含整日可指定 `2026-08-31T23:59:59Z`。
`incremental` 仍只处理原有增量同步，不消费修复队列；可在现有任务计划中增加独立的 `repair` 命令。

## 一致性与恢复

采用 PostgreSQL transactional outbox，无需 Redis，也无需跨 PG/Redis 协调事务。
消费时通过 `FOR UPDATE SKIP LOCKED` 领取记录，并持有源库行锁直到目标提交和本地确认完成。
并发命令会跳过其他进程正在处理的记录，剩余数可能包含这些记录。
连接中断或进程退出会释放事务锁，不需要手动重置永久 `RUNNING` 状态。

两端数据库无法通过普通事务做到同时提交，因此采用“至少一次投递 + 幂等覆盖”：
若生产提交后、本地确认前崩溃，下次重新同步同一范围即可。全部范围修复使用目标库事务级 advisory lock
串行执行，在锁内读取本地 K 线，避免重叠范围的修复因提交顺序不同而覆盖为旧快照。
原有保护仍生效：本地范围完全没有 K 线时拒绝清空生产数据，记录保留为失败待处理。

## 回归测试

普通测试无需数据库，PostgreSQL 集成测试默认跳过。集成测试需显式指定隔离测试库：

```powershell
$env:MARKET_REPAIR_TEST_DATABASE_URL = 'postgresql+psycopg://user:password@127.0.0.1:5432/test_database'
python -m unittest discover -s tests -v
```

集成测试只在该 URL 下创建并清理带随机后缀的测试 schema，验证真实事务、行锁、失败恢复、迁移及同步结果。

# 回测记录分享

部署后端代码前先执行数据库迁移：

```sh
alembic upgrade head
```

新迁移 `e2f4a91c608b` 只新增 `market_backtest_bookmark` 收藏关系表，不复制或修改已有成交记录。先迁移并部署后端，再部署前端。

- 分享链接格式：`https://站点域名/market-master?share=<public_id>`。使用现有随机 UUID；记录默认私有，点击分享后改为仅链接可访问。
- `POST /market_master/backtest/sessions/{public_id}/share`：作者开启分享；已收藏且有权查看的用户可获取同一链接用于转发。
- `DELETE /market_master/backtest/sessions/{public_id}/share`：仅作者可停止分享。
- `POST /market_master/backtest/shared/{public_id}`：登录后收藏并获取完整回放详情；重复请求幂等，自己的记录不会重复收藏。
- 列表和详情接口包含自己的记录及有效收藏；收藏不会赋予写入成交、结束原回测或停止分享的权限。
- 收藏者删除列表项只移除自己的收藏。原作者停止分享或删除原记录后，收藏项显示失效；再次开启分享恢复原链接。正在进行的回测再次打开时会读取最新成交。
- 未登录用户先登录并保留完整回跳地址；成功收藏后移除地址中的 `share` 参数，刷新不会再次强制进入回放。

验证（不连接业务数据库）：

```sh
python -m unittest tests.test_market_backtest tests.test_market_backtest_migration -v
```

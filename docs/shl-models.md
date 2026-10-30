# SHL 模型配置

模型列表保存在 `llms` 表中；SHL 的提供商、分析价格和默认模型统一由
`app/core/shl_models.py` 定义，并通过 `/api_v1/llms` 返回给前端。

| 模型 | 分析点数 |
| --- | ---: |
| gemini-3.8-flash（默认，替换 gemini-3-flash-preview） | 10 |
| gemini-4-argon | 30 |
| gemini-3-pro-preview（保留） | 30 |
| gemini-2.5-pro（保留） | 30 |
| gpt-5.6-sol | 60 |
| gpt-6-astra | 100 |

拍照纠错使用 `gemini-3.8-flash`，沿用 5 点/次的功能定价。

## 部署

1. 配置 `OPENAI_BASE_URL` 为 OpenAI 兼容网关地址，`API_KEY` 为该网关的密钥。
   客户端使用 `settings.openai_base_url` 和 `settings.api_key`，会补齐缺失的 `/v1` 后缀。
   模型名称按上表原样发送，需要网关支持相应模型及图片输入。
2. 在后端环境执行 `python -m alembic upgrade head`，再重启后端并发布前端。
   迁移保留旧 Flash 的记录 ID，更新模型列表，并添加 OpenAI 算力流水类型。
3. 若需要回滚，先检查是否已产生 OpenAI 算力流水；存在流水时迁移会阻止降级，以保护账目兼容性。

OpenAI 调用使用异步 SDK 的 Chat Completions，传递文字、题目图片及可选测试用例图片，
要求 JSON 输出。失败或无效输出继续走现有任务失败退款流程。图片以 data URL 传递，
兼容原始 base64 和已有 data URL。

接口参考：[官方 OpenAI Chat Completions 文档](https://developers.openai.com/api/reference/python/resources/chat/subresources/completions/methods/create)。

## 本地验证

```powershell
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -p test_shl_models.py -v
```

测试使用隔离的 SQLite 数据库和模拟 HTTP 服务，不调用真实模型或发送邮件。

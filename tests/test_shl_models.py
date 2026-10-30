"""SHL gateway, billing and model migration checks without external side effects."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import BackgroundTasks, Request
from openai import AsyncOpenAI
from sqlalchemy import create_engine, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.clients import openai as openai_client
from app.models.ai_task import AITask, TaskStatus
from app.models.llms import LLMs
from app.models.shl_solver import ActionType
from app.models.user import User, UserCredit, UserCreditLog
from app.router.shl_analyze import process_shl_analyze, process_code_verify
from app.schemas.llms import LLMSerializer
from app.schemas.shl_analyze import ImageData, SHLAnalyzePayload, SHLCodeVerifyPayload
from app.services.shl_analyze import shl_service
from app.services import task_worker


PRICES = {
    "gemini-3.8-flash": 10, "gemini-4-argon": 30,
    "gpt-5.6-sol": 60, "gpt-6-astra": 100,
    "gemini-3-pro-preview": 30, "gemini-2.5-pro": 30,
}
ANSWER = {
    "summary": "Sum the inputs", "key_concepts": ["iteration"], "constraints": ["n > 0"],
    "solutions": {"python": "print(sum(map(int, input().split())))"}, "complexity": "O(n)",
}


def load_migration():
    path = Path(__file__).parents[1] / "alembic/versions/a31f6c8d902e_update_shl_models.py"
    spec = importlib.util.spec_from_file_location("shl_models_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AsyncSessionAdapter:
    def __init__(self, session):
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    def add(self, item):
        self.session.add(item)

    async def commit(self):
        self.session.commit()

    async def rollback(self):
        self.session.rollback()

    async def refresh(self, item):
        self.session.refresh(item)


class SHLBillingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        for table in (User.__table__, UserCredit.__table__, UserCreditLog.__table__, LLMs.__table__, AITask.__table__):
            table.create(self.engine)
        self.session = Session(self.engine)
        self.session.add_all([
            User(id=1, username="tester", email="tester@example.com", password="unused"),
            UserCredit(user_id=1, free_credits=200, paid_credits=0),
        ])
        for model_id, key in enumerate(PRICES, 1):
            self.session.add(LLMs(id=model_id, key=key, name=key, tag="test", desc="test", enabled=True))
        self.session.commit()
        self.db = AsyncSessionAdapter(self.session)
        self.request = Request({"type": "http", "path": "/shl_analyze", "headers": [], "client": ("127.0.0.1", 1234)})
        self.request.state.user = SimpleNamespace(id=1)
        self.analytics = patch("app.router.shl_analyze.capture_event")
        self.analytics.start()

    def tearDown(self):
        self.analytics.stop()
        self.session.close()
        self.engine.dispose()

    def wallet(self):
        return self.session.scalar(select(UserCredit).where(UserCredit.user_id == 1))

    async def test_each_model_charges_its_price_and_passes_it_to_refund_worker(self):
        for model_id, (key, price) in enumerate(PRICES.items(), 1):
            with self.subTest(model=key):
                wallet = self.wallet()
                wallet.free_credits = 200
                self.session.commit()
                background = BackgroundTasks()
                response = await process_shl_analyze(self.request, SHLAnalyzePayload(images_data=[], llmId=model_id), background, self.db)
                self.assertEqual(response.code, 200)
                self.assertEqual(wallet.free_credits, 200 - price)
                self.assertEqual(background.tasks[0].kwargs["llm_key"], key)
                self.assertEqual(background.tasks[0].kwargs["cost"], price)
                log = self.session.scalars(select(UserCreditLog).order_by(UserCreditLog.id.desc())).first()
                self.assertEqual(log.amount, -price)
                if key.startswith("gpt-"):
                    self.assertEqual(log.action_type, ActionType.USE_OPENAI_MODEL)
                serialized = LLMSerializer.model_validate(self.session.get(LLMs, model_id)).model_dump()
                self.assertEqual(serialized["cost_credits"], price)
                self.assertEqual(serialized["is_default"], key == "gemini-3.8-flash")
                self.assertEqual(serialized["provider"], "openai" if key.startswith("gpt-") else "gemini")

    async def test_insufficient_balance_never_queues_or_charges(self):
        self.wallet().free_credits = 99
        self.session.commit()
        background = BackgroundTasks()
        response = await process_shl_analyze(self.request, SHLAnalyzePayload(images_data=[], llmId=4), background, self.db)
        self.assertEqual(response.code, 402)
        self.assertEqual(self.wallet().free_credits, 99)
        self.assertFalse(background.tasks)
        self.assertEqual(self.session.scalars(select(UserCreditLog)).all(), [])

    async def test_disabled_and_unknown_models_do_not_consume_credits(self):
        for key, enabled, expected in (("gpt-6-astra", False, 404), ("gpt-unknown", True, 400), ("gemini-3-flash-preview", True, 400)):
            model = self.session.get(LLMs, 4)
            model.key, model.enabled = key, enabled
            self.session.commit()
            background = BackgroundTasks()
            response = await process_shl_analyze(self.request, SHLAnalyzePayload(images_data=[], llmId=4), background, self.db)
            self.assertEqual(response.code, expected)
            self.assertEqual(self.wallet().free_credits, 200)
            self.assertFalse(background.tasks)

    async def test_verification_preserves_five_credit_price(self):
        background = BackgroundTasks()
        response = await process_code_verify(self.request, SHLCodeVerifyPayload(reference_code="print(1)"), background, self.db)
        self.assertEqual(response.code, 200)
        self.assertEqual(self.wallet().free_credits, 195)
        self.assertEqual(background.tasks[0].kwargs["cost"], 5)

    async def test_task_creation_failure_refunds_openai_charge(self):
        with patch.object(self.db, "refresh", side_effect=RuntimeError("task creation failed")):
            with self.assertRaisesRegex(RuntimeError, "task creation failed"):
                await process_shl_analyze(self.request, SHLAnalyzePayload(images_data=[], llmId=4), BackgroundTasks(), self.db)
        self.assertEqual(self.wallet().free_credits, 200)
        logs = self.session.scalars(select(UserCreditLog).order_by(UserCreditLog.id)).all()
        self.assertEqual([log.amount for log in logs], [-100, 100])
        self.assertTrue(all(log.action_type == ActionType.USE_OPENAI_MODEL for log in logs))


class SHLGatewayTest(unittest.IsolatedAsyncioTestCase):
    async def test_real_sdk_sends_selected_model_images_and_test_case_to_v1(self):
        requests = []

        async def handler(request):
            requests.append(request)
            return httpx.Response(200, json={
                "id": "test", "object": "chat.completion", "created": 1, "model": "test",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "```json\n" + json.dumps([ANSWER]) + "\n```"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 8, "total_tokens": 20},
            })

        async with AsyncOpenAI(api_key="test-key", base_url="https://gateway.example/v1", http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler))) as client:
            with patch("app.services.shl_analyze.get_openai_client", return_value=client), patch("app.services.shl_analyze.get_gemini_client") as gemini:
                payload = SHLAnalyzePayload(llmId=3, images_data=[ImageData(mimeType="image/png", data="YQ=="), ImageData(mimeType="image/jpeg", data="data:image/jpeg;base64,Yg==")], test_case_image=ImageData(mimeType="image/png", data="Yw=="))
                for model in ("gpt-5.6-sol", "gpt-6-astra"):
                    result, tokens = await shl_service.analyze(payload, model)
                    self.assertEqual(result, ANSWER)
                    self.assertEqual(tokens, 20)
                    request = requests[-1]
                    self.assertEqual(request.url.path, "/v1/chat/completions")
                    body = json.loads(request.content)
                    self.assertEqual(body["model"], model)
                    self.assertEqual(body["response_format"], {"type": "json_object"})
                    self.assertEqual(body["messages"][0]["role"], "system")
                    content = body["messages"][1]["content"]
                    self.assertIn("ALWAYS PRIORITIZE", content[0]["text"])
                    self.assertEqual([part["image_url"]["url"] for part in content if part["type"] == "image_url"], ["data:image/png;base64,YQ==", "data:image/jpeg;base64,Yg==", "data:image/png;base64,Yw=="])
                gemini.assert_not_called()

    async def test_empty_malformed_and_incomplete_responses_fail_for_refund(self):
        create = AsyncMock()
        client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
        with patch("app.services.shl_analyze.get_openai_client", return_value=client):
            for content in (None, "", "not JSON", "{}", "[]"):
                create.return_value = SimpleNamespace(usage=None, choices=[SimpleNamespace(message=SimpleNamespace(content=content))])
                with self.assertRaises(ValueError):
                    await shl_service.analyze(SHLAnalyzePayload(images_data=[], llmId=4), "gpt-6-astra")

    async def test_gemini_analysis_and_verification_use_new_model(self):
        generate = AsyncMock(return_value=SimpleNamespace(text=json.dumps(ANSWER), usage_metadata=SimpleNamespace(total_token_count=17)))
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate)))
        with patch("app.services.shl_analyze.get_gemini_client", return_value=client), patch("app.services.shl_analyze.get_openai_client") as openai:
            for model in ("gemini-3.8-flash", "gemini-4-argon", "gemini-3-pro-preview"):
                result, tokens = await shl_service.analyze(SHLAnalyzePayload(images_data=[ImageData(mimeType="image/png", data="YQ==")], llmId=1), model)
                self.assertEqual(generate.call_args.kwargs["model"], model)
                self.assertEqual(tokens, 17)
            generate.return_value = SimpleNamespace(text=json.dumps({"summary": "ok", "has_errors": False, "errors": []}), usage_metadata=SimpleNamespace(prompt_token_count=10, candidates_token_count=5))
            result, tokens = await shl_service.verify_code(SHLCodeVerifyPayload(reference_code="print(1)"))
            self.assertEqual(generate.call_args.kwargs["model"], "gemini-3.8-flash")
            self.assertFalse(result.has_errors)
            self.assertEqual(tokens, 15)
            openai.assert_not_called()

    async def test_openai_client_uses_shared_key_and_normalizes_v1_once(self):
        for url in ("https://gateway.example", "https://gateway.example/", "https://gateway.example/v1", "https://gateway.example/v1/"):
            client = SimpleNamespace(close=AsyncMock())
            with patch.object(openai_client, "settings", SimpleNamespace(openai_base_url=url, api_key="shared-key")), patch.object(openai_client, "AsyncOpenAI", return_value=client) as constructor:
                openai_client.init_openai_client()
                constructor.assert_called_once_with(base_url="https://gateway.example/v1", api_key="shared-key")
                self.assertIs(openai_client.get_openai_client(), client)
                await openai_client.close_openai_client()
                client.close.assert_awaited_once()

    async def test_background_failure_refunds_exact_openai_cost(self):
        for model, cost in (("gpt-5.6-sol", 60), ("gpt-6-astra", 100)):
            db = AsyncMock(spec=AsyncSession)
            db.__aenter__.return_value = db
            task = SimpleNamespace(status=TaskStatus.PENDING, error_message=None)
            result = Mock()
            result.scalars.return_value.first.return_value = task
            db.execute.return_value = result
            with patch.object(task_worker.db_client, "async_session", return_value=db), patch.object(task_worker.shl_service, "analyze", side_effect=ValueError("Empty response from LLM")), patch.object(task_worker.wallet_service, "refund_credits", new_callable=AsyncMock) as refund, patch.object(task_worker, "send_email_alert"), patch.object(task_worker, "capture_event"), patch.object(task_worker, "handle_shl_analyze_background_task", new_callable=AsyncMock):
                await task_worker.background_shl_solver_task("test-task", 1, SHLAnalyzePayload(images_data=[], llmId=3), model, cost, ActionType.USE_OPENAI_MODEL, "127.0.0.1", "/shl_analyze")
                self.assertEqual(task.status, TaskStatus.FAILED)
                refund.assert_awaited_once_with(db=db, user_id=1, amount=cost, action_type=ActionType.USE_OPENAI_MODEL)


class SHLModelMigrationTest(unittest.TestCase):
    def test_upgrade_keeps_flash_id_pro_models_and_handles_existing_new_models(self):
        for existing_new in (False, True):
            with self.subTest(existing_new=existing_new):
                engine = create_engine("sqlite://")
                LLMs.__table__.create(engine)
                with engine.begin() as db:
                    db.execute(text("CREATE TABLE user_credit_log (id INTEGER PRIMARY KEY, action_type VARCHAR(30))"))
                    db.execute(LLMs.__table__.insert(), [{"id": 10, "key": "gemini-3-flash-preview", "name": "old", "enabled": True}, {"id": 20, "key": "gemini-3-pro-preview", "name": "pro", "enabled": True}])
                    if existing_new:
                        db.execute(LLMs.__table__.insert().values(id=30, key="gemini-3.8-flash", name="new", enabled=False))
                    with Operations.context(MigrationContext.configure(db)):
                        migration = load_migration()
                        migration.upgrade()
                        migration.upgrade()  # No duplicate rows after retry.
                        models = {row.key: row for row in db.execute(select(LLMs.__table__))}
                        self.assertEqual(models["gemini-3.8-flash"].id, 30 if existing_new else 10)
                        self.assertTrue(models["gemini-3.8-flash"].enabled)
                        self.assertEqual(models["gemini-3-pro-preview"].name, "pro")
                        for key in ("gemini-4-argon", "gpt-5.6-sol", "gpt-6-astra"):
                            self.assertTrue(models[key].enabled)
                        if existing_new:
                            self.assertFalse(models["gemini-3-flash-preview"].enabled)
                        db.execute(text("INSERT INTO user_credit_log VALUES (1, 'USE_OPENAI_MODEL')"))
                        with self.assertRaisesRegex(RuntimeError, "OpenAI credit records"):
                            migration.downgrade()
                        db.execute(text("DELETE FROM user_credit_log"))
                        migration.downgrade()
                        models = {row.key: row for row in db.execute(select(LLMs.__table__))}
                        self.assertTrue(models["gemini-3-flash-preview"].enabled)
                        self.assertFalse(models["gpt-6-astra"].enabled)
                        migration.upgrade()
                        self.assertTrue(db.scalar(select(LLMs.enabled).where(LLMs.key == "gpt-6-astra")))
                engine.dispose()


if __name__ == "__main__":
    unittest.main()

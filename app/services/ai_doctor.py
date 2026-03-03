from app.clients.gemini import get_gemini_client
from google.genai import types
import json
from app.schemas.ai_doctor import AnalyzePayload
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.helpers import base64_to_bytes
from app.prompts.ai_doctor import generate_prompt
from fastapi import Request
from app.services.token_record import token_record_service


class AIDoctorService:
    async def analyze(
        self, request: Request, payload: AnalyzePayload, db: AsyncSession
    ):
        image_bytes = base64_to_bytes(payload.data)
        client = get_gemini_client()
        # 构造提示词
        prompt_text = generate_prompt(payload.explanationStyle)
        # 调用 Gemini API 进行图像分析
        response = client.models.generate_content(
            model=payload.llmKey,
            contents=[
                # 文本 prompt
                types.Part.from_text(text=prompt_text),
                # 以 bytes 形式传入图像
                types.Part.from_bytes(data=image_bytes, mime_type=payload.mimeType),
            ],
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )
        # TODO: 解析 response，提取所需信息，记录使用的 token 数量
        total_token_count = json.loads(response.json())["usage_metadata"][
            "total_token_count"
        ]

        await token_record_service.record_token_usage(
            request, db, total_token_count, model=payload.llmKey
        )

        # 将分析结果返回给调用方
        result = json.loads(response.text)
        return result


ai_doctor_service = AIDoctorService()

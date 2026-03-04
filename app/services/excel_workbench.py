from fastapi import Request
from app.schemas.excel_workbench import TransformRequest, AIResponseSchema
from sqlalchemy.ext.asyncio import AsyncSession
from app.prompts.excel_workbench import generate_prompt
from app.clients.gemini import get_gemini_client
from google.genai import types
import json
from fastapi import HTTPException


class ExcelWorkbenchService:
    async def transform(
        self, request: Request, payload: TransformRequest, db: AsyncSession
    ):
        try:
            client = get_gemini_client()
            system_instruction = generate_prompt(
                columns=payload.columns, sampleRow=payload.sample_row
            )

            response = await client.aio.models.generate_content(
                model="gemini-3-flash-preview",
                contents=f"User Command: {payload.prompt}",
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=AIResponseSchema,
                    # temperature=0.1  # 建议为严格的代码或 JSON 生成设置较低的 temperature
                ),
            )

            if response.parsed:
                # 如果 AIResponseSchema 是 Pydantic 模型，直接利用 FastAPI 的自动序列化返回字典
                return response.parsed.model_dump()
            else:
                # 退回使用 text 手动解析
                return json.loads(response.text)
        except Exception as e:
            # 实际生产中建议记录日志
            raise HTTPException(
                status_code=500, detail=f"AI Processing failed: {str(e)}"
            )


excel_workbench_service = ExcelWorkbenchService()

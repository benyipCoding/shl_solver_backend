from app.prompts.shl_analyze import system_prompt, user_prompt
from app.utils.helpers import base64_to_bytes
from app.schemas.shl_analyze import SHLAnalyzePayload
from google.genai import types
from app.clients.gemini import get_gemini_client
import json
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.token_record import token_record_service
from fastapi import Request


class SHLAnalyzeService:
    async def analyze(
        self,
        request: Request,
        payload: SHLAnalyzePayload,
        db: AsyncSession,
        llm_key: str,
    ):
        """
        images_data expected format:
        [{"mimeType": "image/jpeg", "data": "<base64_encoded_string>"}, ...]
        """
        try:
            contents = [user_prompt]

            for img in payload.images_data:
                mime_type = getattr(img, "mimeType", "image/jpeg")
                base64_data = getattr(img, "data", "")

                # Decode the base64 string from the frontend into raw bytes
                image_bytes = base64_to_bytes(base64_data)

                # Create a Part object using the new SDK's from_bytes method
                image_part = types.Part.from_bytes(
                    data=image_bytes, mime_type=mime_type
                )
                contents.append(image_part)

            # Call the model using the typed GenerateContentConfig
            client = get_gemini_client()
            # ✅ 修改点 1：使用 client.aio.models 替代 client.models
            # ✅ 修改点 2：加上 await 关键字
            response = await client.aio.models.generate_content(
                model=llm_key,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    response_mime_type="application/json",
                ),
            )

            total_token_count = response.usage_metadata.total_token_count
            # 把token数量记录到数据库里，方便后续统计和分析
            # TODO: 可以考虑把每次调用的token数量和用户ID、调用时间等信息一起记录下来，做更细粒度的分析
            await token_record_service.record_token_usage(
                request, db, total_token_count, model=llm_key
            )

            # 增加一点防御性清理逻辑
            raw_text = response.text.strip()
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]

            result = json.loads(raw_text.strip())
            return result, total_token_count

        except Exception as e:
            print(f"Error during SHL analysis: {str(e)}")
            raise e


shl_service = SHLAnalyzeService()

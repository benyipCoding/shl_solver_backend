from app.prompts.shl_analyze import (
    system_prompt,
    user_prompt,
    verify_code_system_template,
    verify_code_user_message,
)
from app.utils.helpers import base64_to_bytes
from app.schemas.shl_analyze import (
    SHLAnalyzePayload,
    SHLCodeVerifyPayload,
    SHLCodeVerifyResult,
)
from google.genai import types
from app.clients.gemini import get_gemini_client

from app.clients.openrouter import get_openrouter_client
import json
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.token_record import token_record_service
from fastapi import Request
import re


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

            # 兼容大模型输出中包含非标准 JSON 转义符（如 LaTeX 中的 \frac）导致 Invalid \escape 的问题
            raw_text = re.sub(r'(?<!\\)\\([^"\\/bfnrtu])', r"\\\\\1", raw_text.strip())

            result = json.loads(raw_text)
            return result, total_token_count

        except Exception as e:
            print(f"Error during SHL analysis: {str(e)}")
            raise e

    async def verify_code(
        self,
        request: Request,
        payload: SHLCodeVerifyPayload,
        db: AsyncSession,
    ) -> SHLCodeVerifyResult:
        try:
            language_display = "Python 3"
            if payload.language == "java":
                language_display = "Java"
            elif payload.language == "javascript":
                language_display = "JavaScript (Node.js)"

            system_instruction = verify_code_system_template.format(
                language_display=language_display
            )

            # Construct user message prompts
            reference_code_part = types.Part.from_text(
                text=f"Here is the reference code for comparison:\n```{payload.language}\n{payload.reference_code}\n```"
            )
            instruction_part = types.Part.from_text(text=verify_code_user_message)

            user_parts = [instruction_part, reference_code_part]

            # Process Image
            if payload.image_data:
                mime_type = getattr(payload.image_data, "mimeType", "image/jpeg")
                base64_data = getattr(payload.image_data, "data", "")

                if base64_data:
                    image_bytes = base64_to_bytes(base64_data)
                    image_part = types.Part.from_bytes(
                        data=image_bytes, mime_type=mime_type
                    )
                    user_parts.append(image_part)

            contents = [types.Content(role="user", parts=user_parts)]

            client = get_gemini_client()
            model_name = "gemini-3-flash-preview"

            response = await client.aio.models.generate_content(
                model=model_name,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                ),
            )

            if not response.text:
                raise ValueError("Empty response from LLM")

            # Clean up potential markdown code blocks in response
            raw_text = response.text.strip()
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]

            result_data = json.loads(raw_text.strip())

            if isinstance(result_data, list):
                result_data = result_data[0] if result_data else {}

            # Count tokens (approximate or get usage metadata if available)
            prompt_token_count = 0
            candidates_token_count = 0
            if response.usage_metadata:
                prompt_token_count = response.usage_metadata.prompt_token_count or 0
                candidates_token_count = (
                    response.usage_metadata.candidates_token_count or 0
                )

            total_tokens = prompt_token_count + candidates_token_count
            await token_record_service.record_token_usage(
                request, db, total_tokens, model=model_name
            )

            return SHLCodeVerifyResult(**result_data)

        except Exception as e:
            print(f"Error during code verification: {str(e)}")
            raise e

    async def analyze_openrouter(
        self,
        request: Request,
        payload: SHLAnalyzePayload,
        db: AsyncSession,
        llm_key: str,
    ):
        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})

            user_content = []
            if user_prompt:
                user_content.append({"type": "text", "text": user_prompt})

            for img in payload.images_data:
                mime_type = getattr(img, "mimeType", "image/jpeg")
                base64_data = getattr(img, "data", "")

                if base64_data:
                    # Constructor for base64 OpenRouter URL
                    image_url = f"data:{mime_type};base64,{base64_data}"
                    user_content.append(
                        {"type": "image_url", "image_url": {"url": image_url}}
                    )
            if user_content:
                messages.append({"role": "user", "content": user_content})

            client = get_openrouter_client()

            response = await client.chat.completions.create(
                model=llm_key,
                messages=messages,
                response_format={"type": "json_object"},
            )

            total_token_count = response.usage.total_tokens if response.usage else 0

            await token_record_service.record_token_usage(
                request, db, total_token_count, model=llm_key
            )

            raw_text = (
                response.choices[0].message.content.strip()
                if response.choices and response.choices[0].message.content
                else "{}"
            )
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]

            result = json.loads(raw_text.strip())
            return result, total_token_count

        except Exception as e:
            print(f"Error during SHL analysis with OpenRouter: {str(e)}")
            raise e

    async def verify_code_openrouter(
        self,
        request: Request,
        payload: SHLCodeVerifyPayload,
        db: AsyncSession,
    ) -> SHLCodeVerifyResult:
        try:
            language_display = "Python 3"
            if payload.language == "java":
                language_display = "Java"
            elif payload.language == "javascript":
                language_display = "JavaScript (Node.js)"

            system_instruction = verify_code_system_template.format(
                language_display=language_display
            )

            messages = [{"role": "system", "content": system_instruction}]
            user_content = []

            # Add instruction and reference code
            user_content.append(
                {
                    "type": "text",
                    "text": f"{verify_code_user_message}\n\nHere is the reference code for comparison:\n```{payload.language}\n{payload.reference_code}\n```",
                }
            )

            # Process Image
            if payload.image_data:
                mime_type = getattr(payload.image_data, "mimeType", "image/jpeg")
                base64_data = getattr(payload.image_data, "data", "")

                if base64_data:
                    image_url = f"data:{mime_type};base64,{base64_data}"
                    user_content.append(
                        {"type": "image_url", "image_url": {"url": image_url}}
                    )

            if user_content:
                messages.append({"role": "user", "content": user_content})

            client = get_openrouter_client()
            model_name = (
                "google/gemini-2.5-flash"  # Default OpenRouter model for verify
            )

            response = await client.chat.completions.create(
                model=model_name,
                messages=messages,
                response_format={"type": "json_object"},
            )

            if not response.choices or not response.choices[0].message.content:
                raise ValueError("Empty response from LLM")

            raw_text = response.choices[0].message.content.strip()

            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]

            result_data = json.loads(raw_text.strip())

            if isinstance(result_data, list):
                result_data = result_data[0] if result_data else {}

            total_tokens = response.usage.total_tokens if response.usage else 0
            await token_record_service.record_token_usage(
                request, db, total_tokens, model=model_name
            )

            return SHLCodeVerifyResult(**result_data)

        except Exception as e:
            print(f"Error during code verification with OpenRouter: {str(e)}")
            raise e


shl_service = SHLAnalyzeService()

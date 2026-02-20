from app.prompts.shl_analyze import system_prompt, user_prompt
from app.utils.helpers import base64_to_bytes
from app.schemas.shl_analyze import SHLAnalyzePayload
from google.genai import types
from app.clients.gemini import get_gemini_client
import json


class SHLAnalyzeService:
    def analyze(self, payload: SHLAnalyzePayload):
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
            response = client.models.generate_content(
                model=payload.llmKey,  # Pro models are recommended for deep reasoning and coding tasks
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    response_mime_type="application/json",
                ),
            )

            total_token_count = json.loads(response.json())["usage_metadata"][
                "total_token_count"
            ]
            print(f"Total token count: {total_token_count}")
            result = json.loads(response.text)
            return result

        except Exception as e:
            print(f"Error during SHL analysis: {str(e)}")
            raise e


shl_service = SHLAnalyzeService()

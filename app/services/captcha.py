from captcha.image import ImageCaptcha
import uuid
from app.clients.redis_client import get_redis
import random
import string


class CaptchaService:
    def generate_key(self, captcha_id: str) -> str:
        return f"captcha:{captcha_id}"

    def generate_code(self, length=5) -> str:
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

    async def generate_captcha(self, old_id: str) -> tuple[str, bytes]:
        r = get_redis()

        if old_id:
            await self.delete_captcha(old_id)

        # 生成验证码
        code = self.generate_code()

        # 生成唯一ID
        captcha_id = str(uuid.uuid4())

        # 存入 Redis（3分钟过期）
        await r.setex(self.generate_key(captcha_id), 180, code)

        # 生成图片
        image = ImageCaptcha()
        data = image.generate(code)

        return captcha_id, data

    async def validate_captcha(self, user_input: str, captcha_id: str) -> bool:
        r = get_redis()
        key = self.generate_key(captcha_id)
        stored_code = await r.get(key)
        if stored_code is None:
            return False
        if stored_code.lower() != user_input.lower():
            return False

        # 校验成功后删除（防止重复使用）
        await r.delete(key)
        return True

    async def delete_captcha(self, captcha_id: str):
        r = get_redis()
        key = self.generate_key(captcha_id)
        if await r.exists(key):
            await r.delete(key)
        else:
            return


captcha_service = CaptchaService()

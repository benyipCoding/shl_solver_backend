from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from captcha.image import ImageCaptcha
import random
import string
import uuid
from app.clients.redis_client import get_redis


router = APIRouter(prefix="/captcha", tags=["Captcha"])


def generate_code(length=5):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


@router.get("/")
def get_captcha():
    r = get_redis()

    # 生成验证码
    code = generate_code()

    # 生成唯一ID
    captcha_id = str(uuid.uuid4())

    # 存入 Redis（5分钟过期）
    r.setex(f"captcha:{captcha_id}", 300, code)

    # 生成图片
    image = ImageCaptcha()
    data = image.generate(code)

    return StreamingResponse(
        data, media_type="image/png", headers={"Captcha-Id": captcha_id}
    )

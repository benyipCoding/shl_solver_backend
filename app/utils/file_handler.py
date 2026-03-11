import os
import uuid
import base64
import mimetypes
from app.schemas.shl_analyze import ImageData

# 【新增】定义存储路径，指向我们配置好的 Docker 共享数据卷
UPLOAD_DIR = "/app/uploads/shl_images"


# 【新增】抽离出一个专门用来保存 Base64 图片的辅助函数
def save_images_to_disk(images_data: list[ImageData]):
    # 1. 确保目录存在 (如果 uploads 已经有 777 权限，这一步会自动成功建出 shl_images)
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    for img in images_data:
        # 2. 清理可能带有的 Base64 前缀 (例如前端传来的 "data:image/png;base64,iVBORw0...")
        b64_data = img.data
        if "," in b64_data:
            b64_data = b64_data.split(",")[1]

        # 3. 将 Base64 解码为二进制流
        try:
            image_bytes = base64.b64decode(b64_data)
        except Exception as e:
            print(f"Base64 解码失败: {e}")
            continue  # 某一张失败了跳过，继续存下一张

        # 4. 根据 mimeType 获取对应的文件后缀名 (比如 image/jpeg -> .jpg)
        ext = mimetypes.guess_extension(img.mimeType) or ".png"

        # 5. 生成唯一文件名并写入硬盘
        filename = f"{uuid.uuid4().hex}{ext}"
        filepath = os.path.join(UPLOAD_DIR, filename)

        try:
            with open(filepath, "wb") as f:
                f.write(image_bytes)
        except Exception as e:
            print(f"文件保存失败: {e}")

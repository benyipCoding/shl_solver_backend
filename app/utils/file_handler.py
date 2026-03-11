import os
import uuid
import base64
import json
import mimetypes
from app.schemas.shl_analyze import ImageData
from app.clients import db  # 修改为导入模块，从而可以使用 db.async_session
from app.models.shl_solver import SHLSolverHistory
from sqlalchemy.ext.asyncio import AsyncSession

# 【新增】定义存储路径，指向我们配置好的 Docker 共享数据卷
UPLOAD_DIR = "/app/uploads/shl_images"


# 【新增】抽离出一个专门用来保存 Base64 图片的辅助函数
def save_images_to_local(images_data: list[ImageData]) -> list[str]:
    """
    将图片列表保存到本地磁盘，并返回保存后的文件相对路径列表
    """
    # 1. 确保目录存在
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    saved_paths = []

    for img in images_data:
        # 2. 清理可能带有的 Base64 前缀
        b64_data = img.data
        if "," in b64_data:
            b64_data = b64_data.split(",")[1]

        # 3. 将 Base64 解码为二进制流
        try:
            image_bytes = base64.b64decode(b64_data)
        except Exception as e:
            print(f"Base64 解码失败: {e}")
            continue

        # 4. 根据 mimeType 获取对应的文件后缀名
        ext = mimetypes.guess_extension(img.mimeType) or ".png"

        # 5. 生成唯一文件名并写入硬盘
        filename = f"{uuid.uuid4().hex}{ext}"
        filepath = os.path.join(UPLOAD_DIR, filename)

        try:
            with open(filepath, "wb") as f:
                f.write(image_bytes)
            # 记录相对路径或者 URL
            saved_paths.append(f"shl_images/{filename}")
        except Exception as e:
            print(f"文件保存失败: {e}")

    return saved_paths


async def save_shl_history_to_db(
    user_id: int,
    model: str,
    token_count: int,
    result_data: dict,
    image_paths: list[str],
):
    """
    保存 SHL 分析的历史记录到数据库
    """
    try:
        if db.async_session is None:
            print("Error: db.async_session is None")
            return

        async with db.async_session() as session:
            history = SHLSolverHistory(
                image_urls=",".join(image_paths),
                token_count=token_count,
                model=model,
                user_id=user_id,
                result_json=json.dumps(result_data, ensure_ascii=False),
                status="completed",
            )
            session.add(history)
            await session.commit()
    except Exception as e:
        print(f"保存历史记录失败: {e}")


# 【新增】封装给 background_task 调用的统一入口函数
async def handle_shl_analyze_background_task(
    images_data: list[ImageData],
    user_id: int,
    model: str,
    token_count: int,
    result_data: dict,
):
    """
    处理 SHL 分析后的后台任务：保存图片 + 记录历史
    """
    # 1. 先同步保存图片（虽然是 I/O 操作，但在 background task 中运行不会阻塞主线程响应）
    # 注意：如果 convert 过程很慢，也可以考虑把 save_images_to_local 改成 async 并使用 aiofiles
    # 但这里为了复用简单逻辑，暂且保持同步 IO，在线程池或后台任务中跑也没问题
    saved_paths = save_images_to_local(images_data)

    # 2. 再异步保存数据库记录
    await save_shl_history_to_db(user_id, model, token_count, result_data, saved_paths)

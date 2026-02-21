FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 安装系统级依赖
# 比如 gcc 和 libpq-dev 是编译安装 PostgreSQL 驱动通常需要的
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# 请确保您的主入口文件是 main.py，并且 FastAPI 实例名为 app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
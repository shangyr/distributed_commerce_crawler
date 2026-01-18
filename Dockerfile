# 基于Python 3.12的基础镜像
FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    redis-tools \
    --no-install-recommends

# 安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 配置环境变量
ENV PYTHONUNBUFFERED=1
ENV SCRAPY_LOG_LEVEL=INFO

# 暴露端口
EXPOSE 6800  
EXPOSE 8000  

# 启动命令（使用supervisor管理多个进程）
CMD ["sh", "-c", "scrapyd && tail -f /dev/null"]
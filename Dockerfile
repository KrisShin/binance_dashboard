# Dockerfile

# 1. 使用官方的 Python 3.10 slim 镜像作为基础
FROM python:3.10-slim

# 2. 在容器内创建一个 /app 目录作为工作目录
WORKDIR /app

# 3. 复制依赖文件到容器中
#    我们先复制这个文件，以便利用Docker的缓存机制
COPY requirements.txt .

# 4. 安装所有依赖
RUN pip install --no-cache-dir -r requirements.txt

# 5. 复制你项目中的所有文件 (所有.py文件, index.html) 到容器的 /app 目录
COPY . .

# 6. 这个镜像是灵活的，我们不在Dockerfile中固定启动命令
#    而是在 docker-compose.yml 中指定要运行哪个.py文件
CMD ["python", "-c", "print('错误：请在docker-compose中指定要运行的命令')"]

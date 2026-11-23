FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# SQLite 数据文件放到挂载卷目录，容器重建数据不丢
ENV SCHED_DB_PATH=/app/data/sched.db
RUN mkdir -p /app/data

EXPOSE 5000

# 容器启动时幂等建表，再起服务；threaded 开启以验证并发提交
CMD ["python", "-c", "from app.app import app, init_db; init_db(); app.run(host='0.0.0.0', port=5000, threaded=True)"]

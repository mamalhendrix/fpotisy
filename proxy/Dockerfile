FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir --default-timeout=100 -i https://package-mirror.liara.ir/repository/pypi/simple fastapi uvicorn httpx python-multipart mutagen httpx[socks] deezspot
COPY main.py /app/main.py
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

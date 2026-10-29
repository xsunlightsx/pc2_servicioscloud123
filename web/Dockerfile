# Construir DESDE la raíz del proyecto:  docker build -f web/Dockerfile -t web-ventas:1.0 .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY web/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY web/web.py .
COPY web/static ./static
COPY data/referencia.json .
CMD ["python", "web.py"]

# Construir DESDE la raíz del proyecto:  docker build -f generador/Dockerfile -t generador-ventas:1.0 .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY generador/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generador/generador.py .
COPY data/ventas.csv ./data/ventas.csv
CMD ["python", "generador.py"]

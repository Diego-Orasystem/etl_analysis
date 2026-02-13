FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY cliente_websocket_analysis.py ./

CMD ["python", "cliente_websocket_analysis.py"]
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY cliente_websocket_analysis.py /app/cliente_websocket_analysis.py

CMD ["python", "/app/cliente_websocket_analysis.py"]
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.ws.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY cliente_websocket_analysis.py /app/cliente_websocket_analysis.py

CMD ["python", "/app/cliente_websocket_analysis.py"]

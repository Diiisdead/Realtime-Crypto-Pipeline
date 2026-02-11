# confluent-kafka cần librdkafka để build
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/src/ ./src/

ENV SYMBOLS=btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt,adausdt,dogeusdt,dotusdt,avaxusdt,linkusdt
ENV KAFKA_BOOTSTRAP=kafka:29092
ENV KAFKA_TOPIC=price-ticks

CMD ["python", "src/main.py"]

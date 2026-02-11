# Real-time Crypto Pipeline

Real-time price pipeline: ingest from Binance WebSocket → Kafka → Flink (MA5, MA20) → TimescaleDB → Grafana.

---

## System Architecture

<div align="center" style="margin: 1.5em 0; background:#fff;">

<table cellspacing="0" cellpadding="0" border="0" align="center" style="background:#fff;">
<tr>
  <td align="center" style="padding:16px; min-width:100px; background:#fff; vertical-align:middle; border:1px solid #ddd; border-radius:8px;">
    <div style="font-weight:bold; color:#333; margin-bottom:12px; font-size:14px;">Data Sources</div>
    <img src="assets/binance-logo.svg" width="48" height="48" alt="Binance"/><br/>
    <span style="font-size:13px; color:#333;">Binance WebSocket</span>
  </td>
  <td style="padding:0 8px; font-size:20px; color:#999; vertical-align:middle;">→</td>
  <td align="center" style="padding:16px; min-width:120px; background:#fff; vertical-align:middle; border:1px solid #ddd; border-radius:8px;">
    <div style="font-weight:bold; color:#333; margin-bottom:12px; font-size:14px;">Ingestion</div>
    <img src="assets/python-logo.svg" width="48" height="48" alt="Python"/><br/>
    <span style="font-size:13px; color:#333;">Producer (Python)</span>
    <div style="margin-top:12px;"><img src="assets/kafka-logo.svg" width="48" height="48" alt="Kafka"/><br/>
    <span style="font-size:13px; color:#333;">Kafka</span></div>
  </td>
  <td style="padding:0 8px; font-size:20px; color:#999; vertical-align:middle;">→</td>
  <td align="center" style="padding:16px; min-width:120px; background:#fff; vertical-align:middle; border:1px solid #ddd; border-radius:8px;">
    <div style="font-weight:bold; color:#333; margin-bottom:12px; font-size:14px;">Stream Processing</div>
    <img src="assets/flink-logo.svg" width="48" height="48" alt="Flink"/><br/>
    <span style="font-size:13px; color:#333;">Apache Flink</span>
    <div style="margin-top:12px; font-size:13px; color:#333;">MA5 / MA20</div>
  </td>
  <td style="padding:0 8px; font-size:20px; color:#999; vertical-align:middle;">→</td>
  <td align="center" style="padding:16px; min-width:100px; background:#fff; vertical-align:middle; border:1px solid #ddd; border-radius:8px;">
    <div style="font-weight:bold; color:#333; margin-bottom:12px; font-size:14px;">Storage</div>
    <img src="assets/timescaledb-logo.svg" width="48" height="48" alt="TimescaleDB"/><br/>
    <span style="font-size:13px; color:#333;">TimescaleDB</span>
  </td>
  <td style="padding:0 8px; font-size:20px; color:#999; vertical-align:middle;">→</td>
  <td align="center" style="padding:16px; min-width:100px; background:#fff; vertical-align:middle; border:1px solid #ddd; border-radius:8px;">
    <div style="font-weight:bold; color:#333; margin-bottom:12px; font-size:14px;">Visualization</div>
    <img src="assets/grafana-logo.svg" width="48" height="48" alt="Grafana"/><br/>
    <span style="font-size:13px; color:#333;">Grafana</span>
  </td>
</tr>
</table>

</div>

- **Producer**: Connects to Binance WebSocket, normalizes ticker → JSON, publishes to Kafka topic `price-ticks` (auto-reconnects on disconnect).
- **Flink**: Consumes `price-ticks`, keys by symbol, keeps last 20 prices, computes MA(5) and MA(20), writes to table `price_ticks`.
- **Grafana**: Queries TimescaleDB, displays price, MAs and volume by symbol and time range.

For data flow, components and deployment details: **[ARCHITECTURE.md](ARCHITECTURE.md)**.

---

## Requirements

| Component | Notes |
|-----------|--------|
| **Docker + Docker Compose** | Run Kafka, TimescaleDB, Grafana, Producer. |
| **Java 17** | Build and run Flink JAR. |
| **Maven** | Build: `mvn clean package -DskipTests`. |

*(To run Producer on host: Python 3 and `librdkafka` required, e.g. macOS: `brew install librdkafka`.)*

---

## Quick Start

### 1. Start infrastructure

From the project root:

```bash
docker compose up -d
```

Wait for services to become healthy. Topic `price-ticks` is created automatically.

### 2. Run Producer (Binance → Kafka)

**With Docker (recommended):**

```bash
docker compose up -d producer
```

**On host:**

```bash
cd producer
pip install -r requirements.txt
SYMBOLS=btcusdt,ethusdt KAFKA_BOOTSTRAP=localhost:9092 python src/main.py
```

Symbol list can be changed in `docker-compose.yml` (service `producer` → `environment.SYMBOLS`).

### 3. Build and run Flink (Kafka → MA → TimescaleDB)

```bash
cd stream-processor
mvn clean package -DskipTests
```

Run the JAR:

```bash
export KAFKA_BOOTSTRAP=localhost:9092
export JDBC_URL=jdbc:postgresql://localhost:5432/pipeline
export JDBC_USER=pipeline
export JDBC_PASSWORD=pipeline_secret
java -jar target/stream-processor-1.0-SNAPSHOT.jar
```

The job runs continuously: reads Kafka → computes MA(5), MA(20) → writes to TimescaleDB.

### 4. View Grafana dashboard

1. Open **http://localhost:3000**
2. Log in: **admin** / **admin**
3. Go to **Dashboards** → **Price Pipeline - Real-time**
4. Select **Symbol** and time range.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SYMBOLS` | Binance pairs (btcusdt, ethusdt, ...) | btcusdt, ethusdt, bnbusdt, solusdt, ... |
| `KAFKA_BOOTSTRAP` | Kafka broker | localhost:9092 (host) / kafka:29092 (Docker) |
| `KAFKA_TOPIC` | Raw ticks topic | price-ticks |
| `JDBC_URL` | TimescaleDB URL (Flink) | jdbc:postgresql://localhost:5432/pipeline |
| `JDBC_USER` | DB user | pipeline |
| `JDBC_PASSWORD` | DB password | pipeline_secret |

---

## Default Credentials

Defined in `docker-compose.yml`:

| Service | User | Password |
|---------|------|----------|
| **TimescaleDB** | pipeline | pipeline_secret |
| **Grafana** | admin | admin |

Use the same `JDBC_USER` / `JDBC_PASSWORD` when running the Flink job.

---

## Directory Structure

```
project/
├── docker-compose.yml     # Zookeeper, Kafka, TimescaleDB, Grafana, Producer
├── producer/              # Python: Binance WS → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py
│       ├── fetcher/       # binance_ws.py
│       └── publisher/     # kafka_publisher.py
├── stream-processor/      # Flink: Kafka → MA → TimescaleDB
│   ├── pom.xml
│   └── src/main/java/com/pipeline/jobs/
│       ├── MovingAverageJob.java
│       ├── MovingAverageFunction.java
│       ├── PriceTick.java
│       └── PriceWithMA.java
├── storage/init/          # DB schema (runs on container startup)
│   └── 01_schema.sql
├── grafana/provisioning/  # Datasource + dashboard
│   ├── datasources/
│   └── dashboards/
├── ARCHITECTURE.md        # Detailed architecture
└── README.md
```

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Source | Binance WebSocket API |
| Queue | Apache Kafka (topic `price-ticks`) |
| Stream processing | Apache Flink 1.18 (KeyedProcessFunction, JDBC Sink) |
| Storage | TimescaleDB (PostgreSQL, hypertable) |
| Dashboard | Grafana (PostgreSQL datasource) |
| Producer | Python (websocket-client, confluent-kafka) |

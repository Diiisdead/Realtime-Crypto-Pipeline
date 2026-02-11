# System architecture — Real-time Crypto/Stock Pipeline

## 1. Overview

The system processes real-time price data: ingest from the exchange, push to a message queue, compute indicators (Moving Average) on the stream, store time-series data, and display on a dashboard.

```mermaid
flowchart LR
  subgraph sources [Data Sources]
    Binance[Binance WebSocket]
  end
  subgraph ingestion [Ingestion]
    Producer[Producer Python]
    Kafka[Kafka]
  end
  subgraph processing [Stream Processing]
    Flink[Apache Flink]
    MA[Moving Average MA5 MA20]
  end
  subgraph storage [Storage]
    TimescaleDB[TimescaleDB]
  end
  subgraph viz [Visualization]
    Grafana[Grafana]
  end
  Binance --> Producer --> Kafka --> Flink --> MA --> TimescaleDB --> Grafana
```

---

## 2. Data flow

```mermaid
sequenceDiagram
  participant Binance as Binance API
  participant Producer as Producer
  participant Kafka as Kafka
  participant Flink as Flink Job
  participant TSDB as TimescaleDB
  participant Grafana as Grafana

  Binance->>Producer: Ticker events (WebSocket)
  Producer->>Producer: Normalize to JSON
  Producer->>Kafka: Produce to topic price-ticks
  Kafka->>Flink: Consume messages
  Flink->>Flink: KeyBy symbol, state buffer
  Flink->>Flink: Compute MA(5), MA(20)
  Flink->>TSDB: INSERT price_ticks
  Grafana->>TSDB: SQL queries
  TSDB->>Grafana: Time series results
```

---

## 3. Layer details

### 3.1 Data source

| Component | Description |
|-----------|-------------|
| **Binance WebSocket** | 24h ticker stream (`/ws/<symbol>@ticker`). Each event has current price (c), volume (v), time (E). |
| **Output format** | Producer normalizes to `{ symbol, timestamp (ISO), price, volume }`. |

### 3.2 Ingestion

```mermaid
flowchart LR
  subgraph producer [Producer]
    WS[WebSocket Client]
    Norm[Normalize]
    Send[Kafka Producer]
    WS --> Norm --> Send
  end
  Send -->|price-ticks| Kafka[Kafka Topic]
```

- **Producer** (Python): Connects to Binance WebSocket, receives ticks → normalizes → publishes to Kafka.
- **Kafka**: Topic `price-ticks`, partition key = symbol. Supports multiple consumers, replay, and independent scaling of producer/consumer.

### 3.3 Stream processing (Flink)

```mermaid
flowchart TB
  subgraph flink [Flink Job]
    Source[KafkaSource]
    Parse[Parse JSON]
    KeyBy[KeyBy symbol]
    MAFunc[KeyedProcessFunction]
    State[ListState: last 20 prices]
    Sink[JdbcSink]
    Source --> Parse --> KeyBy --> MAFunc
    MAFunc --> State
    State --> MAFunc
    MAFunc --> Sink
  end
  KafkaTopic[price-ticks] --> Source
  Sink --> TSDB[TimescaleDB]
```

- **KafkaSource**: Reads `price-ticks`, deserializes JSON to `PriceTick`.
- **KeyedProcessFunction**: For each key (symbol), keeps ListState of the last 20 prices; on each event updates state, computes MA(5) and MA(20), emits `PriceWithMA`.
- **JdbcSink**: Writes directly to table `price_ticks` (time, symbol, price, volume, ma_short, ma_long).

### 3.4 Storage (TimescaleDB)

- **Table**: `price_ticks` — hypertable partitioned by `time`.
- **Index**: `(symbol, time DESC)` for queries by symbol and time range.
- **Protocol**: PostgreSQL; Flink uses JDBC.

### 3.5 Visualization (Grafana)

- **Datasource**: PostgreSQL (TimescaleDB), proxied through Grafana.
- **Dashboard**: Time series for price + MA(5) + MA(20); volume panel; `symbol` variable (queried from `price_ticks`).

---

## 4. Deployment (Docker)

```mermaid
flowchart TB
  subgraph docker [Docker Compose]
    subgraph infra [Infrastructure]
      ZK[Zookeeper 2181]
      Kafka[Kafka 9092]
      TSDB[TimescaleDB 5432]
      Grafana[Grafana 3000]
    end
    ZK --> Kafka
  end

  subgraph host [Host / IDE]
    ProducerApp[Producer Python]
    FlinkJob[Flink JAR]
  end

  ProducerApp -->|localhost:9092| Kafka
  FlinkJob -->|localhost:9092| Kafka
  FlinkJob -->|localhost:5432| TSDB
  Grafana -->|timescaledb:5432| TSDB
```

| Service | Port | Notes |
|---------|------|--------|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092 (host), 29092 (internal) | Broker |
| TimescaleDB | 5432 | Pipeline DB |
| Grafana | 3000 | Dashboard UI |

Producer and Flink job run outside Docker (on host or in IDE), connecting to Kafka and TimescaleDB via `localhost`.

---

## 5. Directory structure and responsibilities

```mermaid
flowchart LR
  subgraph repo [Repository]
    Compose[docker-compose.yml]
    ProducerDir[producer/]
    FlinkDir[stream-processor/]
    StorageDir[storage/]
    GrafanaDir[grafana/]
  end
  Compose --> Infra[Zookeeper Kafka TSDB Grafana]
  ProducerDir --> Producer[Binance WS to Kafka]
  FlinkDir --> Flink[MA job to TimescaleDB]
  StorageDir --> Schema[DB schema init]
  GrafanaDir --> Dash[Datasource and dashboards]
```

| Directory / File | Responsibility |
|------------------|----------------|
| `docker-compose.yml` | Start Zookeeper, Kafka, TimescaleDB, Grafana |
| `producer/` | Fetch from Binance WebSocket, publish JSON to Kafka |
| `stream-processor/` | Flink job: Kafka → MA → TimescaleDB |
| `storage/init/` | TimescaleDB schema (hypertable, index) |
| `grafana/provisioning/` | Datasource + dashboard JSON |

---

## 6. Technology stack

| Layer | Technology |
|-------|------------|
| Source | Binance WebSocket API |
| Ingestion | Kafka (topic `price-ticks`) |
| Processing | Apache Flink 1.18 (KafkaSource, KeyedProcessFunction, JdbcSink) |
| Storage | TimescaleDB (PostgreSQL + hypertable) |
| Visualization | Grafana (PostgreSQL datasource) |
| Producer | Python (websocket-client, confluent-kafka) |

---

## 7. Future extensions (suggestions)

- Add sources: CoinGecko / Yahoo Finance (REST or WebSocket) via an additional producer or dedicated topic.
- Flink: Increase parallelism; add indicators (RSI, Bollinger) in the same job or a new job.
- TimescaleDB: Continuous aggregates for OHLCV by minute/hour.
- Grafana: Alerts when price crosses a threshold or when MAs cross.

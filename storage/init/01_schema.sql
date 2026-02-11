-- TimescaleDB schema for price ticks with moving averages
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS price_ticks (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,
    ma_short    DOUBLE PRECISION,
    ma_long     DOUBLE PRECISION
);

SELECT create_hypertable('price_ticks', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_price_ticks_symbol_time ON price_ticks (symbol, time DESC);

COMMENT ON TABLE price_ticks IS 'Real-time price ticks with MA(5) and MA(20) computed by Flink';

package com.pipeline.jobs;

import java.io.Serializable;
import java.time.Instant;

/** Canonical price tick from Kafka (JSON). */
public class PriceTick implements Serializable {
    private String symbol;
    private String timestamp;  // ISO-8601
    private double price;
    private double volume;

    public PriceTick() {}

    public PriceTick(String symbol, String timestamp, double price, double volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
        this.volume = volume;
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }
    public double getVolume() { return volume; }
    public void setVolume(double volume) { this.volume = volume; }
}

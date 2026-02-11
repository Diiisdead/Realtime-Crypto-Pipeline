package com.pipeline.jobs;

import java.io.Serializable;
import java.time.Instant;

/** Price tick with moving averages for TimescaleDB sink. */
public class PriceWithMA implements Serializable {
    private Instant time;
    private String symbol;
    private double price;
    private double volume;
    private Double maShort;  // MA(5)
    private Double maLong;   // MA(20)

    public PriceWithMA() {}

    public PriceWithMA(Instant time, String symbol, double price, double volume, Double maShort, Double maLong) {
        this.time = time;
        this.symbol = symbol;
        this.price = price;
        this.volume = volume;
        this.maShort = maShort;
        this.maLong = maLong;
    }

    public Instant getTime() { return time; }
    public void setTime(Instant time) { this.time = time; }
    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }
    public double getVolume() { return volume; }
    public void setVolume(double volume) { this.volume = volume; }
    public Double getMaShort() { return maShort; }
    public void setMaShort(Double maShort) { this.maShort = maShort; }
    public Double getMaLong() { return maLong; }
    public void setMaLong(Double maLong) { this.maLong = maLong; }
}

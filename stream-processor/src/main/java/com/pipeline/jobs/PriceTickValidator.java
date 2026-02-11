package com.pipeline.jobs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Validation for PriceTick: symbol non-empty, price and volume >= 0, timestamp ISO-8601 parseable.
 */
public final class PriceTickValidator {

    private PriceTickValidator() {}

    /**
     * Returns true if the tick has valid symbol (non-null, non-blank), price >= 0, volume >= 0,
     * and valid timestamp (non-null, non-empty, ISO-8601 parseable).
     */
    public static boolean isValid(PriceTick t) {
        if (t == null) return false;
        if (t.getSymbol() == null || t.getSymbol().isBlank()) return false;
        if (t.getPrice() < 0 || t.getVolume() < 0) return false;
        return hasValidTimestamp(t.getTimestamp());
    }

    /**
     * Returns a list of error messages for the tick; empty list if valid.
     */
    public static List<String> getErrors(PriceTick t) {
        List<String> errors = new ArrayList<>();
        if (t == null) {
            errors.add("tick is null");
            return errors;
        }
        if (t.getSymbol() == null || t.getSymbol().isBlank())
            errors.add("symbol is null or blank");
        if (t.getPrice() < 0) errors.add("price < 0");
        if (t.getVolume() < 0) errors.add("volume < 0");
        if (!hasValidTimestamp(t.getTimestamp()))
            errors.add("timestamp is null, empty, or not ISO-8601");
        return errors;
    }

    /** Returns true if the string is non-null, non-empty and parseable as ISO-8601 instant. */
    public static boolean hasValidTimestamp(String ts) {
        if (ts == null || ts.isEmpty()) return false;
        try {
            Instant.parse(ts);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Returns epoch millis for the timestamp, or Long.MIN_VALUE if invalid. */
    public static long parseTimestampToMillis(String ts) {
        if (ts == null || ts.isEmpty()) return Long.MIN_VALUE;
        try {
            return Instant.parse(ts).toEpochMilli();
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }
}

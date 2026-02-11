package com.pipeline.jobs;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for parsing (parseTick) and validation (PriceTickValidator, hasValidTimestamp, parseTimestampToMillis).
 */
class MovingAverageJobTest {

    @Test
    void parseTick_validJson_returnsPriceTick() {
        String json = "{\"symbol\":\"btcusdt\",\"timestamp\":\"2024-01-15T10:00:00Z\",\"price\":42000.5,\"volume\":100.2}";
        PriceTick t = MovingAverageJob.parseTick(json);
        assertNotNull(t);
        assertEquals("btcusdt", t.getSymbol());
        assertEquals("2024-01-15T10:00:00Z", t.getTimestamp());
        assertEquals(42000.5, t.getPrice(), 1e-9);
        assertEquals(100.2, t.getVolume(), 1e-9);
    }

    @Test
    void parseTick_missingFields_usesDefaults() {
        String json = "{\"symbol\":\"eth\"}";
        PriceTick t = MovingAverageJob.parseTick(json);
        assertNotNull(t);
        assertEquals("eth", t.getSymbol());
        assertNull(t.getTimestamp());
        assertEquals(0.0, t.getPrice(), 1e-9);
        assertEquals(0.0, t.getVolume(), 1e-9);
    }

    @Test
    void parseTick_invalidJson_returnsNull() {
        assertNull(MovingAverageJob.parseTick("not json"));
        assertNull(MovingAverageJob.parseTick("{"));
        assertNull(MovingAverageJob.parseTick(null));
    }

    @Test
    void hasValidTimestamp_validIso8601_returnsTrue() {
        assertTrue(PriceTickValidator.hasValidTimestamp("2024-01-15T10:00:00Z"));
        assertTrue(PriceTickValidator.hasValidTimestamp("2024-01-15T10:00:00.123Z"));
    }

    @Test
    void hasValidTimestamp_nullOrEmpty_returnsFalse() {
        assertFalse(PriceTickValidator.hasValidTimestamp(null));
        assertFalse(PriceTickValidator.hasValidTimestamp(""));
    }

    @Test
    void hasValidTimestamp_invalidFormat_returnsFalse() {
        assertFalse(PriceTickValidator.hasValidTimestamp("not-a-date"));
        assertFalse(PriceTickValidator.hasValidTimestamp("2024-13-45")); // invalid month/day
    }

    @Test
    void parseTimestampToMillis_valid_returnsMillis() {
        long ms = PriceTickValidator.parseTimestampToMillis("2024-01-15T10:00:00Z");
        assertTrue(ms > 0);
        assertEquals(1705312800000L, ms);
    }

    @Test
    void parseTimestampToMillis_invalid_returnsLongMinValue() {
        assertEquals(Long.MIN_VALUE, PriceTickValidator.parseTimestampToMillis(null));
        assertEquals(Long.MIN_VALUE, PriceTickValidator.parseTimestampToMillis(""));
        assertEquals(Long.MIN_VALUE, PriceTickValidator.parseTimestampToMillis("invalid"));
    }

    @Test
    void validator_isValid_validTick_returnsTrue() {
        PriceTick t = new PriceTick("btcusdt", "2024-01-15T10:00:00Z", 42000.0, 100.0);
        assertTrue(PriceTickValidator.isValid(t));
    }

    @Test
    void validator_isValid_nullTick_returnsFalse() {
        assertFalse(PriceTickValidator.isValid(null));
    }

    @Test
    void validator_isValid_blankSymbol_returnsFalse() {
        assertFalse(PriceTickValidator.isValid(new PriceTick("", "2024-01-15T10:00:00Z", 1.0, 1.0)));
        assertFalse(PriceTickValidator.isValid(new PriceTick("   ", "2024-01-15T10:00:00Z", 1.0, 1.0)));
        assertFalse(PriceTickValidator.isValid(new PriceTick(null, "2024-01-15T10:00:00Z", 1.0, 1.0)));
    }

    @Test
    void validator_isValid_negativePrice_returnsFalse() {
        assertFalse(PriceTickValidator.isValid(new PriceTick("btc", "2024-01-15T10:00:00Z", -1.0, 1.0)));
    }

    @Test
    void validator_isValid_negativeVolume_returnsFalse() {
        assertFalse(PriceTickValidator.isValid(new PriceTick("btc", "2024-01-15T10:00:00Z", 1.0, -1.0)));
    }

    @Test
    void validator_isValid_invalidTimestamp_returnsFalse() {
        assertFalse(PriceTickValidator.isValid(new PriceTick("btc", null, 1.0, 1.0)));
        assertFalse(PriceTickValidator.isValid(new PriceTick("btc", "", 1.0, 1.0)));
        assertFalse(PriceTickValidator.isValid(new PriceTick("btc", "invalid", 1.0, 1.0)));
    }

    @Test
    void validator_getErrors_validTick_returnsEmpty() {
        PriceTick t = new PriceTick("btcusdt", "2024-01-15T10:00:00Z", 42000.0, 100.0);
        assertTrue(PriceTickValidator.getErrors(t).isEmpty());
    }

    @Test
    void validator_getErrors_invalidTick_returnsMessages() {
        PriceTick t = new PriceTick("", "bad", -1.0, -2.0);
        List<String> errors = PriceTickValidator.getErrors(t);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("symbol")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("price")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("volume")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("timestamp")));
    }
}

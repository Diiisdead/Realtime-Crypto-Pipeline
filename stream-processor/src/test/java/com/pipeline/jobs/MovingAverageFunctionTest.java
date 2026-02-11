package com.pipeline.jobs;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MovingAverageFunction using Flink test harness.
 * Tests main output (MA values) and late-ticks side output.
 */
class MovingAverageFunctionTest {

    private static final OutputTag<PriceTick> LATE_TICKS = new OutputTag<PriceTick>("late-ticks") {};
    private static final long ALLOWED_LATENESS_MS = 5_000L;

    private OneInputStreamOperatorTestHarness<PriceTick, PriceWithMA> harness;

    @BeforeEach
    void setUp() throws Exception {
        MovingAverageFunction function = new MovingAverageFunction(ALLOWED_LATENESS_MS, LATE_TICKS);
        KeyedProcessOperator<String, PriceTick, PriceWithMA> operator =
            new KeyedProcessOperator<>(function);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
            operator,
            PriceTick::getSymbol,
            Types.STRING
        );
        harness.open();
    }

    @Test
    void singleTick_emitsPriceWithMA_equalToPrice() throws Exception {
        PriceTick tick = new PriceTick("btcusdt", "2024-01-15T10:00:00Z", 100.0, 10.0);
        harness.processElement(tick, 1000L);

        List<PriceWithMA> output = harness.extractOutputValues();
        assertEquals(1, output.size());
        PriceWithMA out = output.get(0);
        assertEquals("btcusdt", out.getSymbol());
        assertEquals(100.0, out.getPrice(), 1e-9);
        assertEquals(100.0, out.getMaShort(), 1e-9);
        assertEquals(100.0, out.getMaLong(), 1e-9);
    }

    @Test
    void fiveTicks_maShortIsAverageOfLastFive() throws Exception {
        String ts = "2024-01-15T10:00:00Z";
        for (int i = 1; i <= 5; i++) {
            harness.processElement(new PriceTick("btcusdt", ts, 100.0 * i, 1.0), 1000L + i);
        }

        List<PriceWithMA> output = harness.extractOutputValues();
        assertEquals(5, output.size());
        // Last output: prices were 100, 200, 300, 400, 500 -> MA(5) = 300, MA(20) = 300
        PriceWithMA last = output.get(4);
        assertEquals(300.0, last.getMaShort(), 1e-9);
        assertEquals(300.0, last.getMaLong(), 1e-9);
    }

    @Test
    void lateTick_goesToSideOutput_notToMain() throws Exception {
        // Advance watermark past event time + allowed lateness
        long watermark = 20_000L; // e.g. 20 sec
        harness.processWatermark(watermark);

        // Tick with event time 1000 (way before watermark - 5000 allowed lateness)
        PriceTick lateTick = new PriceTick("btcusdt", "2024-01-15T10:00:00Z", 99.0, 1.0);
        harness.processElement(lateTick, 1000L);

        List<PriceWithMA> mainOutput = harness.extractOutputValues();
        assertTrue(mainOutput.isEmpty());

        List<PriceTick> sideOutput = streamRecordsToTicks(harness.getSideOutput(LATE_TICKS));
        assertEquals(1, sideOutput.size());
        assertEquals("btcusdt", sideOutput.get(0).getSymbol());
        assertEquals(99.0, sideOutput.get(0).getPrice(), 1e-9);
    }

    @Test
    void onTimeTick_afterWatermark_goesToMainOutput() throws Exception {
        // Tick at 1000
        harness.processElement(
            new PriceTick("btcusdt", "2024-01-15T10:00:00Z", 100.0, 1.0),
            1000L
        );
        // Watermark at 2000 (tick 1000 is still within allowed lateness if we consider watermark 2000 - 5000 = -3000, so 1000 > -3000, not late)
        harness.processWatermark(2000L);
        // Another tick at 3000
        harness.processElement(
            new PriceTick("btcusdt", "2024-01-15T10:00:05Z", 200.0, 1.0),
            3000L
        );

        List<PriceWithMA> mainOutput = harness.extractOutputValues();
        assertEquals(2, mainOutput.size());
        List<PriceTick> sideOutput = streamRecordsToTicks(harness.getSideOutput(LATE_TICKS));
        assertTrue(sideOutput.isEmpty());
    }

    private static List<PriceTick> streamRecordsToTicks(Iterable<StreamRecord<PriceTick>> records) {
        List<PriceTick> list = new ArrayList<>();
        if (records != null) {
            for (StreamRecord<PriceTick> sr : records) {
                list.add(sr.getValue());
            }
        }
        return list;
    }

    @Test
    void twoSymbols_separateState() throws Exception {
        String ts = "2024-01-15T10:00:00Z";
        harness.processElement(new PriceTick("btcusdt", ts, 100.0, 1.0), 1000L);
        harness.processElement(new PriceTick("ethusdt", ts, 50.0, 1.0), 1001L);
        harness.processElement(new PriceTick("btcusdt", ts, 200.0, 1.0), 1002L);

        List<PriceWithMA> output = harness.extractOutputValues();
        assertEquals(3, output.size());
        // btcusdt: 100 then 200 -> second has maShort=maLong=150
        assertEquals("btcusdt", output.get(0).getSymbol());
        assertEquals(100.0, output.get(0).getPrice(), 1e-9);
        assertEquals("ethusdt", output.get(1).getSymbol());
        assertEquals(50.0, output.get(1).getPrice(), 1e-9);
        assertEquals("btcusdt", output.get(2).getSymbol());
        assertEquals(200.0, output.get(2).getPrice(), 1e-9);
        assertEquals(150.0, output.get(2).getMaShort(), 1e-9);
        assertEquals(150.0, output.get(2).getMaLong(), 1e-9);
    }
}

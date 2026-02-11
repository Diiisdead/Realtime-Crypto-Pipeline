package com.pipeline.jobs;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * KeyedProcessFunction: for each symbol, keep last 20 prices in state;
 * on each event compute MA(5) and MA(20) and emit PriceWithMA.
 */
public class MovingAverageFunction extends KeyedProcessFunction<String, PriceTick, PriceWithMA> {

    private static final int MA_SHORT_WINDOW = 5;
    private static final int MA_LONG_WINDOW = 20;

    private transient ListState<Double> priceHistoryState;

    @Override
    public void open(Configuration parameters) {
        priceHistoryState = getRuntimeContext().getListState(
            new ListStateDescriptor<>("prices", Double.class)
        );
    }

    @Override
    public void processElement(PriceTick tick, KeyedProcessFunction<String, PriceTick, PriceWithMA>.Context ctx, Collector<PriceWithMA> out) throws Exception {
        List<Double> prices = new ArrayList<>();
        for (Double p : priceHistoryState.get()) {
            prices.add(p);
        }
        prices.add(tick.getPrice());
        if (prices.size() > MA_LONG_WINDOW) {
            prices = prices.subList(prices.size() - MA_LONG_WINDOW, prices.size());
        }
        priceHistoryState.clear();
        for (Double p : prices) {
            priceHistoryState.add(p);
        }

        double maShort = prices.size() >= MA_SHORT_WINDOW
            ? prices.subList(prices.size() - MA_SHORT_WINDOW, prices.size()).stream().mapToDouble(Double::doubleValue).average().orElse(0)
            : prices.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double maLong = prices.stream().mapToDouble(Double::doubleValue).average().orElse(0);

        Instant time = parseTimestamp(tick.getTimestamp());
        PriceWithMA withMA = new PriceWithMA(
            time,
            tick.getSymbol(),
            tick.getPrice(),
            tick.getVolume(),
            maShort,
            maLong
        );
        out.collect(withMA);
    }

    private static Instant parseTimestamp(String ts) {
        if (ts == null || ts.isEmpty()) return Instant.now();
        try {
            return Instant.parse(ts);
        } catch (Exception e) {
            return Instant.now();
        }
    }
}

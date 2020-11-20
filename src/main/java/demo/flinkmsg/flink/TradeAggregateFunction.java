package demo.flinkmsg.flink;

import demo.flinkmsg.entity.Trade;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class TradeAggregateFunction implements AggregateFunction<Tuple2<Integer, Trade>, List<Trade>, List<Trade>> {
    @Override
    public List<Trade> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Trade> add(Tuple2<Integer, Trade> value, List<Trade> accumulator) {
        accumulator.add(value.f1);
        return accumulator;
    }

    @Override
    public List<Trade> getResult(List<Trade> accumulator) {
        List<Trade> order = accumulator.stream().sorted(Comparator.comparing(Trade::getTradeVersion)).collect(Collectors.toList());
        return order;
    }

    @Override
    public List<Trade> merge(List<Trade> a, List<Trade> b) {
        List<Trade> all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }
}

package demo.flinkmsg;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;
import demo.flinkmsg.exception.MismatchException;
import demo.flinkmsg.exception.TradeValueDiffException;
import demo.flinkmsg.exception.VersionOutOfOrderException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseTradeMessageTest {

    public void verify(List<TradeMessage> origin, List<Trade> trades) {
        verifySameSize(origin, trades);
        verifySameValues(origin, trades);
        verifyVersionTimestamps(trades);
    }

    public void verifySameSize(List<TradeMessage> origin, List<Trade> trades) {
        if ((null == origin || null == trades)) {
            throw new MismatchException("Input trades has invalid value");
        }

        if (origin.size() != trades.size()) {
            throw new MismatchException("Input trades has size mismatch[" + origin.size() + "," + trades.size() + "]");
        }
    }

    public void verifySameValues(List<TradeMessage> origin, List<Trade> trades) {
        Map<String, Trade> tradeMap = trades.stream().collect(
                Collectors.toMap((trade) -> trade.getTradeId() + "-" + trade.getTradeVersion(), (trade) -> trade));
        origin.parallelStream().forEach(msg -> {
            Trade trade = (Trade) msg.getObject();
            Trade tradeInDb = tradeMap.get(trade.getTradeId() + "-" + trade.getTradeVersion());
            boolean same = null != tradeInDb && trade.getCusip().equals(tradeInDb.getCusip())
                    && 0 == ObjectUtils.compare(trade.getAmount(), tradeInDb.getAmount())
                    && 0 == ObjectUtils.compare(trade.getPrice(), tradeInDb.getPrice())
                    && trade.getCreateDate().getTime() == tradeInDb.getCreateDate().getTime();
            if (!same) {
                throw new TradeValueDiffException("Trade compare with origin error: " + ReflectionToStringBuilder.toString(trade) + "->" + ReflectionToStringBuilder.toString(tradeInDb));
            }
        });
    }

    public void verifyVersionTimestamps(List<Trade> trades) {
        Map<Integer, List<Trade>> groupById = trades.stream().collect(Collectors.groupingBy(trade -> trade.getTradeId()));
        groupById.entrySet().stream().map(en -> en.getValue()).forEach(versions -> {
            //sorted in case it is not
            List<Trade> test = versions.stream().sorted(Comparator.comparing(trade -> trade.getTradeVersion())).collect(Collectors.toList());
            for (int i = 0; i < test.size(); i++) {
                Trade current = test.get(i);
                for (int j = i + 1; j < test.size(); j++) {
                    Trade trade = test.get(j);
                    if (0 < current.getProcessDate().compareTo(trade.getProcessDate())) {
                        System.out.println("*****************************");
                        throw new VersionOutOfOrderException("Trade version in db error: " + ReflectionToStringBuilder.toString(current) + "->" + ReflectionToStringBuilder.toString(trade));
                    }
                }
            }
        });
    }

    protected AssignerWithPeriodicWatermarks<TradeMessage> assignTimestampsAndWatermarks(long maxOutOfOrder) {
        AssignerWithPeriodicWatermarks<TradeMessage> assign = new AssignerWithPeriodicWatermarks<TradeMessage>() {
            private long messageTimestamp = new Date().getTime();

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(messageTimestamp - maxOutOfOrder);
            }

            @Override
            public long extractTimestamp(TradeMessage element, long previousElementTimestamp) {
                messageTimestamp = element.getJMSTimestamp();

                return messageTimestamp;

            }
        };
        return assign;
    }

    protected AggregateFunction<Tuple2<Integer, Trade>, List<Trade>, List<Trade>> tradesAggregateFunction() {
        AggregateFunction<Tuple2<Integer, Trade>, List<Trade>, List<Trade>> function = new AggregateFunction<Tuple2<Integer, Trade>, List<Trade>, List<Trade>>() {

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
        };
        return function;
    }
}

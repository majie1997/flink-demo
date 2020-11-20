package demo.flinkmsg;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;
import demo.flinkmsg.service.TradeService;
import demo.flinkmsg.sink.DataSourceSinkFunction;
import demo.flinkmsg.util.TradeEntityGeneratorUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SpringBootTest(classes = {FlinkApplication.class})
@RunWith(SpringRunner.class)
public class TradeVersionLegacyTest {
    private final static Logger logger = LoggerFactory.getLogger(TradeVersionLegacyTest.class);
    @Autowired
    private TradeService tradeService;
    //    @Autowired
//    private H2SinkFunction sinkFunction;
    @Autowired
    private DataSourceSinkFunction sinkFunction;

    private final int tradeSize = 5000;
    private final int maxVersion = 10;


    @Test
    @Sql(value = {"classpath:db/table.ddl.sql"})
    public void testEventTimeWindows() throws Exception {
        final String name = "testEventTimeWindows";
        logger.info(name + " starts");
        MapFunction<TradeMessage, List<Trade>> toListMapper = new MapFunction<TradeMessage, List<Trade>>(){
            @Override
            public List<Trade> map(TradeMessage msg) throws Exception {
                List<Trade> trades = new ArrayList<>();
                trades.add((Trade)msg.getObject());
                return trades;
            }
        };
        List<TradeMessage> input = TradeEntityGeneratorUtil.generateTradesWithinObjectMessage(tradeSize, maxVersion);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
        DataStreamSource<TradeMessage> dataStreamSource = env.fromCollection(input);

        dataStreamSource.assignTimestampsAndWatermarks(assignTimestampsAndWatermarks()).map(toListMapper).addSink(sinkFunction);
        env.execute(name);

        verify(name, input, tradeService.findAll());
        logger.info(name + " completed");
    }

    @Test
    @Sql(value = {"classpath:db/table.ddl.sql"})
    public void testEventTimeWindowsWithOrderedData() throws Exception {
        final String name = "testEventTimeWindowsWithOrderedData";
        logger.info(name + " starts");

        List<TradeMessage> input = TradeEntityGeneratorUtil.generateTradesWithinObjectMessage(tradeSize, maxVersion);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
        DataStreamSource<TradeMessage> dataStreamSource = env.fromCollection(input);
        dataStreamSource.assignTimestampsAndWatermarks(assignTimestampsAndWatermarks())
                .map(mapper()).keyBy(v -> v.f0)
                .timeWindow(Time.seconds(5))
                .aggregate(tradesAggregateFunction())
                .addSink(sinkFunction);
        env.execute(name);

        verify(name, input, tradeService.findAll());
        logger.info(name + " completed");
    }

    @Test
    @Sql(value = {"classpath:db/table.ddl.sql"})
    public void testEventTimeWindowsWithUnOrderedData() throws Exception {
        final String name = "testEventTimeWindowsWithUnOrderedData";
        logger.info(name + " starts");

        List<TradeMessage> input = TradeEntityGeneratorUtil.generateTradesWithinObjectMessage(tradeSize, maxVersion, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
        DataStreamSource<TradeMessage> dataStreamSource = env.fromCollection(input);
        dataStreamSource.assignTimestampsAndWatermarks(assignTimestampsAndWatermarks())
                .map(mapper()).keyBy(v -> v.f0)
                .timeWindow(Time.seconds(5))
                .aggregate(tradesAggregateFunction())
                .addSink(sinkFunction);
        env.execute(name);

        verify(name, input, tradeService.findAll());
        logger.info(name + " completed");
    }

    private void verify(String testcase, List<TradeMessage> origin, List<Trade> trades) {
        verifySameSize(testcase, origin, trades);
        verifySameValues(testcase, origin, trades);
        verifyVersionTimestamps(testcase, trades);
    }

    private void verifySameSize(String testcase, List<TradeMessage> origin, List<Trade> trades) {
        if ((null == origin || null == trades)) {
            throw new RuntimeException(testcase + " has invalid value");
        }

        if (origin.size() != trades.size()) {
            throw new RuntimeException(testcase + " has size mismatch");
        }
    }

    private void verifySameValues(String testcase, List<TradeMessage> origin, List<Trade> trades) {
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
                throw new RuntimeException(testcase + ": trade compare with origin error: " + ReflectionToStringBuilder.toString(trade) + "->" + ReflectionToStringBuilder.toString(tradeInDb));
            }
        });
    }

    private void verifyVersionTimestamps(String testcase, List<Trade> trades) {
        Map<Integer, List<Trade>> groupById = trades.stream().collect(Collectors.groupingBy(trade -> trade.getTradeId()));
        groupById.entrySet().stream().map(en -> en.getValue()).forEach(versions -> {
            //sorted in case it is not
            versions.stream().sorted(Comparator.comparing(trade -> trade.getTradeVersion()));
            for (int i = 0; i < versions.size(); i++) {
                Trade current = versions.get(i);
                for (int j = i + 1; j < versions.size(); j++) {
                    Trade trade = versions.get(j);
                    if (0 < current.getProcessDate().compareTo(trade.getProcessDate())) {
                        throw new RuntimeException(testcase + ": trade version in db error: " + ReflectionToStringBuilder.toString(current) + "->" + ReflectionToStringBuilder.toString(trade));
                    }
                }
            }

        });
    }


    private AggregateFunction<Tuple2<Integer, Trade>, List<Trade>, List<Trade>> tradesAggregateFunction() {
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
                return accumulator;
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

    private ReduceFunction<Tuple2<Integer, Trade>> aggregateFunction() {
        ReduceFunction<Tuple2<Integer, Trade>> function = new ReduceFunction<Tuple2<Integer, Trade>>() {
            @Override
            public Tuple2<Integer, Trade> reduce(Tuple2<Integer, Trade> value1, Tuple2<Integer, Trade> value2) throws Exception {
                return null;
            }
        };
        return function;
    }

//    private SinkFunction<List<Trade>> sinkFunction() {
//        return new SinkFunction<List<Trade>>() {
//            @Override
//            public void invoke(List<Trade> trades, Context context) throws Exception {
//                logger.info("trade size for sink " + trades.size());
//                tradeService.save(trades.toArray(new Trade[0]));
//            }
//        };
//    }

    private MapFunction<TradeMessage, Tuple2<Integer, Trade>> mapper() {
        return new MapFunction<TradeMessage, Tuple2<Integer, Trade>>() {
            @Override
            public Tuple2<Integer, Trade> map(TradeMessage msg) throws Exception {
                Trade trade = (Trade) msg.getObject();
                return new Tuple2(trade.getTradeId(), trade);
            }
        };
    }

    private AssignerWithPeriodicWatermarks<TradeMessage> assignTimestampsAndWatermarks() {
        AssignerWithPeriodicWatermarks<TradeMessage> assign = new AssignerWithPeriodicWatermarks<TradeMessage>() {
            private long messageTimestamp = new Date().getTime();

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(messageTimestamp);
            }

            @Override
            public long extractTimestamp(TradeMessage element, long previousElementTimestamp) {
                return element.getJMSTimestamp();

            }
        };
        return assign;
    }


}

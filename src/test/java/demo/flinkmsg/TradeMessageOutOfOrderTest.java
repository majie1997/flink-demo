package demo.flinkmsg;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;
import demo.flinkmsg.exception.VersionOutOfOrderException;
import demo.flinkmsg.service.TradeService;
import demo.flinkmsg.sink.DataSourceSinkFunction;
import demo.flinkmsg.util.TradeEntityGenerator;
import demo.flinkmsg.util.TradeEntityGeneratorUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SpringBootTest(classes = {FlinkApplication.class})
@RunWith(SpringRunner.class)
public class TradeMessageOutOfOrderTest extends BaseTradeMessageTest {
    private final int max = 10;
    private final int maxVersion = 10;
    private final long maxOutOfOrder = 500L;

    @Autowired
    private TradeService tradeService;
    @Autowired
    private DataSourceSinkFunction sinkFunction;

    @Test(expected = VersionOutOfOrderException.class)
    @Sql(value = {"classpath:db/table.ddl.sql"})
    public void testOutOfOrderWithoutKeyBy() throws Exception {
        List<TradeMessage> msgs = TradeEntityGenerator.startFrom(new Date()).maxSize(max).maxVersion(maxVersion).maxOutOfOrder(maxOutOfOrder).randomVersionOrder(true).generate();

        MapFunction<TradeMessage, List<Trade>> toListMapper = new MapFunction<TradeMessage, List<Trade>>() {
            @Override
            public List<Trade> map(TradeMessage msg) throws Exception {
                List<Trade> trades = new ArrayList<>();
                trades.add((Trade) msg.getObject());
                return trades;
            }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromCollection(msgs).assignTimestampsAndWatermarks(assignTimestampsAndWatermarks(500L)).map(toListMapper).addSink(sinkFunction);
        env.execute("test");

        verify(msgs, tradeService.findAll());
    }

    @Test
    @Sql(value = {"classpath:db/table.ddl.sql"})
    public void testOutOfOrderWithTimeWindow() throws Exception {
        int timeWindowSize = 5;
        List<TradeMessage> msgs = TradeEntityGenerator.startFrom(new Date()).maxSize(10).maxVersion(maxVersion).maxOutOfOrder(maxOutOfOrder).randomVersionOrder(true).generate();
        MapFunction<TradeMessage, Tuple2<Integer, Trade>> toListMapper = new MapFunction<TradeMessage, Tuple2<Integer, Trade>>() {
                @Override
                public Tuple2<Integer, Trade> map(TradeMessage msg) throws Exception {
                    Trade trade = (Trade) msg.getObject();
                    return new Tuple2(trade.getTradeId(), trade);
                }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromCollection(msgs).assignTimestampsAndWatermarks(assignTimestampsAndWatermarks(0))
                .map(toListMapper).keyBy(v -> v.f0)
                .timeWindow(Time.seconds(timeWindowSize))
                .aggregate(tradesAggregateFunction())
                .addSink(sinkFunction);
        env.execute("test");

        verify(msgs, tradeService.findAll());
    }

    @Test
    @Sql(value = {"classpath:db/table.ddl.sql"})
    public void testOutOfOrderWithTimeWindow2() throws Exception {
        int largeVersion = 10;
        int timeWindowSize = 50;
        List<TradeMessage> msgs = TradeEntityGenerator.startFrom(new Date()).maxSize(max).maxVersion(largeVersion).maxOutOfOrder(maxOutOfOrder, 100, 300).randomVersionOrder(true).generate();
        MapFunction<TradeMessage, Tuple2<Integer, Trade>> toListMapper = new MapFunction<TradeMessage, Tuple2<Integer, Trade>>() {
            @Override
            public Tuple2<Integer, Trade> map(TradeMessage msg) throws Exception {
                Trade trade = (Trade) msg.getObject();
                return new Tuple2(trade.getTradeId(), trade);
            }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<List<Trade>> stream = env.fromCollection(msgs)
                .assignTimestampsAndWatermarks(assignTimestampsAndWatermarks(0))
                .map(toListMapper)
                .keyBy(v -> v.f0)
                .timeWindow(Time.seconds(timeWindowSize))
                .aggregate(tradesAggregateFunction());
        DataStreamSink<List<Trade>> sink = stream
                .addSink(sinkFunction);
        env.execute("test2");
        verify(msgs, tradeService.findAll());



    }



}

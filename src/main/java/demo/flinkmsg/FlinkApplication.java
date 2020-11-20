package demo.flinkmsg;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;
import demo.flinkmsg.flink.TimestampsAndWatermarks;
import demo.flinkmsg.flink.TradeAggregateFunction;
import demo.flinkmsg.sink.DataSourceSinkFunction;
import demo.flinkmsg.sink.H2SinkFunction;
import demo.flinkmsg.util.TradeEntityGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Date;
import java.util.List;

@SpringBootApplication
public class FlinkApplication {
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final Logger logger = LoggerFactory.getLogger(FlinkApplication.class);
    public static void main(String[] args) throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        new SpringApplicationBuilder(FlinkApplication.class)
//                .web(WebApplicationType.NONE)
//                .run(args);
//        int maxVersion = Integer.parseInt(args[1]);
//        int timeWindowSize = Integer.parseInt(args[2]);
//        int maxMsgSize = Integer.parseInt(args[3]);
//        int maxOutOfOrder = Integer.parseInt(args[4]);
        int maxVersion = 10;
        int timeWindowSize = 50;
        int maxMsgSize = 50;
        long maxOutOfOrder = 500L;
        List<TradeMessage> msgs = TradeEntityGenerator.startFrom(new Date()).maxSize(maxMsgSize).maxVersion(maxVersion).maxOutOfOrder(maxOutOfOrder, 100, 300).randomVersionOrder(true).generate();
        MapFunction<TradeMessage, Tuple2<Integer, Trade>> toListMapper = new MapFunction<TradeMessage, Tuple2<Integer, Trade>>() {
            @Override
            public Tuple2<Integer, Trade> map(TradeMessage msg) throws Exception {
                Trade trade = (Trade) msg.getObject();
                return new Tuple2(trade.getTradeId(), trade);
            }
        };
        DataStream<List<Trade>> stream = env.fromCollection(msgs)
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks(maxOutOfOrder))
                .map(toListMapper)
                .keyBy(v -> v.f0)
                .timeWindow(Time.seconds(timeWindowSize))
                .aggregate(new TradeAggregateFunction());
        DataStreamSink<List<Trade>> sink = stream
                .addSink(new DataSourceSinkFunction());
//        .addSink(new SinkFunction<List<Trade>>() {
//            @Override
//            public void invoke(List<Trade> value, Context context) throws Exception {
//                for(Trade t : value){
//                    logger.info("Trade"+"    ID:"+t.getTradeId()+"   Version:"+t.getTradeVersion());
////                    System.out.println("Trade"+"    ID:"+t.getTradeId()+"   Version:"+t.getTradeVersion());
//                }
//            }
//        });
        env.execute("test2");
    }
}

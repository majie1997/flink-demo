package demo.flinkmsg.flink;

import demo.flinkmsg.entity.TradeMessage;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.Date;

public class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<TradeMessage> {
    private long messageTimestamp = new Date().getTime();
    private long maxOutOfOrder =0L;

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

    public TimestampsAndWatermarks(long maxOutOfOrder){this.maxOutOfOrder = maxOutOfOrder;}
}

package demo.flinkmsg.util;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.text.RandomStringGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TradeEntityGenerator {
    private static RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    private int maxSize = 100;
    private int maxVersion = 3;
    private boolean randomVersionOrder = false;
    private long maxOutOfOrder = 100L;
    private int maxOutOfOrderStart = 0;
    private int maxOutOfOrderEnd = 1;
    private long startTime = 0L;

    private TradeEntityGenerator(long startTime) {
        this.startTime = startTime;
    }

    public static TradeEntityGenerator startFrom(Date date) {
        return new TradeEntityGenerator(date.getTime());
    }

    public TradeEntityGenerator maxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public TradeEntityGenerator maxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
        return this;
    }

    public TradeEntityGenerator randomVersionOrder(boolean randomVersionOrder) {
        this.randomVersionOrder = randomVersionOrder;
        return this;
    }

    public TradeEntityGenerator maxOutOfOrder(long maxOutOfOrder) {
        this.maxOutOfOrder = maxOutOfOrder;
        return this;
    }

    public TradeEntityGenerator maxOutOfOrder(long maxOutOfOrder, int start, int end) {
        this.maxOutOfOrder = maxOutOfOrder;
        this.maxOutOfOrderStart = start;
        this.maxOutOfOrderEnd = end;
        return this;
    }

    public List<TradeMessage> generate() {
        return generateTrades();
    }

    private List<TradeMessage> generateTrades() {
        //maxSize / maxVersion + 1 might generate extra trades but will be sublist at the end anyway
        long startTimestamp = startTime;
        List<TradeMessage> msgs = new ArrayList<>();
        for (int i = 0; i < maxSize / maxVersion + 1; i++) {
            int tradeId = 10000 + i;
            List<TradeMessage> versions = generateWithTradeId( tradeId,  startTimestamp);
            msgs.addAll(versions);
            startTimestamp = versions.parallelStream().mapToLong(t->t.getJMSTimestamp()).max().getAsLong();
        }

        return msgs.subList(0, maxSize);
    }

    private List<TradeMessage> generateWithTradeId(Integer tradeId, long startTimestamp) {
        String cusip = generateCusip();
        //create the versions and random shuffled if needed
        List<Trade> trades = IntStream.range(1, maxVersion + 1).mapToObj(v -> {
            Trade trade = new Trade();
            trade.setTradeId(tradeId);
            trade.setTradeVersion(v);
            trade.setAmount(20000D);
            trade.setCreateDate(new Date());
            trade.setCusip(cusip);
            trade.setPrice(100D);
            return trade;
        }).collect(Collectors.toList());
        if (randomVersionOrder) {
            Collections.shuffle(trades);
        }
        long begin = startTimestamp;
        List<TradeMessage> msgs = new ArrayList<>(maxVersion);
        for (int i = 0; i < maxVersion; i++) {
            long jmsTimestamp = begin + maxOutOfOrder + RandomUtils.nextInt(maxOutOfOrderStart, maxOutOfOrderEnd);
            TradeMessage msg = new TradeMessage(trades.get(i), jmsTimestamp);
            msgs.add(msg);
            begin = jmsTimestamp;
        }
        return msgs;
    }

    private static String generateCusip() {
        int length = 9;
        return generator.generate(length);
    }
}

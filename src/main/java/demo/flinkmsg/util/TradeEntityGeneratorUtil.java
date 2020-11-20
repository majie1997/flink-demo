package demo.flinkmsg.util;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;
import org.apache.commons.text.RandomStringGenerator;

import javax.jms.ObjectMessage;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Deprecated
public class TradeEntityGeneratorUtil {
    private static RandomStringGenerator generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    public static List<TradeMessage> generateTradesWithinObjectMessage(int max, int maxVersion) {
        return generateTradesWithinObjectMessage(max, maxVersion, false);
    }

    public static List<TradeMessage> generateTradesWithinObjectMessage(int max, int maxVersion, boolean randomVersionOrder) {
        List<Trade> trades = generateTrades(max, maxVersion, randomVersionOrder);
        List<TradeMessage> messages = trades.stream().map(trade -> new TradeMessage(trade, new Date().getTime())).collect(Collectors.toList());
        return messages;
    }

    public static List<Trade> generateTrades(int max, int maxVersion) {
        return generateTrades(max, maxVersion, false);
    }


    public static List<Trade> generateTrades(int max, int maxVersion, boolean randomVersionOrder) {
        int tradeIdSize = max / maxVersion == 0 ? max / maxVersion : max / maxVersion + 1;
        List<Trade> trades = IntStream.range(0, tradeIdSize).mapToObj(i -> i).flatMap(
                id -> generateWithTradeId(10000 + id, maxVersion, randomVersionOrder).stream()
        ).collect(Collectors.toList());
        return trades.subList(0, max);
    }

    private static List<Trade> generateWithTradeId(Integer tradeId, int maxVersion, boolean randomVersionOrder) {
        String cusip = generateCusip();
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
        if(randomVersionOrder) {
            Collections.shuffle(trades);
        }
        return trades;
    }


    private static String generateCusip() {
        int length = 9;
        return generator.generate(length);
    }
}

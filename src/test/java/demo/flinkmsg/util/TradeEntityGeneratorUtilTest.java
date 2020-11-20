package demo.flinkmsg.util;

import demo.flinkmsg.entity.Trade;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TradeEntityGeneratorUtilTest {

    @Test
    public void testGenerateTrades() {
        int max = 100;
        int maxVersion =3;
        List<Trade> trades = TradeEntityGeneratorUtil.generateTrades(max, maxVersion);
        assertNotNull(trades);
        assertTrue(trades.get(0).getTradeVersion() ==1);
        assertTrue(trades.get(1).getTradeVersion() ==2);
    }
}

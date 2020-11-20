package demo.flinkmsg.util;

import demo.flinkmsg.entity.TradeMessage;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TradeEntityGeneratorTest {

    @Test
    public void testGenerate() {
        final int max = 50000;
        final int maxVersion = 10;
        final long maxOutOfOrder = 500L;
        List<TradeMessage> msgs = TradeEntityGenerator.startFrom(new Date()).maxSize(max).maxVersion(maxVersion).maxOutOfOrder(maxOutOfOrder).generate();

        assertNotNull(msgs);
        assertTrue(max == msgs.size());
        for (int i = 0; i < msgs.size(); i++) {
            if (i > 0) {
                long lastTimestamp = msgs.get(i - 1).getJMSTimestamp();
                long currentTimestamp = msgs.get(i).getJMSTimestamp();
                assertTrue(currentTimestamp - lastTimestamp == maxOutOfOrder);
            }
        }
    }

    @Test
    public void testGenerateWithRandomOutOfOrder() {
        final int max = 50000;
        final int maxVersion = 10;
        final long maxOutOfOrder = 500L;
        final int maxOutOfOrderStart = 100;
        final int maxOutOfOrderEnd = 500;
        List<TradeMessage> msgs = TradeEntityGenerator.startFrom(new Date()).maxSize(max).maxVersion(maxVersion).maxOutOfOrder(maxOutOfOrder, maxOutOfOrderStart, maxOutOfOrderEnd).generate();

        assertNotNull(msgs);
        assertTrue(max == msgs.size());
        for (int i = 0; i < msgs.size(); i++) {
            if (i > 0) {
                long lastTimestamp = msgs.get(i - 1).getJMSTimestamp();
                long currentTimestamp = msgs.get(i).getJMSTimestamp();
                long diff= currentTimestamp - lastTimestamp - maxOutOfOrder;
                assertTrue(diff>= maxOutOfOrderStart && diff < maxOutOfOrderEnd );
            }
        }
    }
}

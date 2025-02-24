package step.core.async;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncProcessorTest {

    public static class MyObject {
        public final String value;

        public MyObject(String value) {
            this.value = value;
        }
    }

    @Test
    public void testAsyncObjectProcessorBasic() {
        testAsyncObjectProcessor(1000, 100, false);
    }

    @Test
    public void testAsyncObjectProcessorQueueLimit() {
        testAsyncObjectProcessor(1, 100, false);
    }

    @Test
    public void testAsyncObjectProcessorQueueLimitWithGracefulClose() {
        testAsyncObjectProcessor(1, 100, true);
    }

    public void testAsyncObjectProcessor(int queueSize, int nbObjects, boolean testClose) {
        List<MyObject> processed = new ArrayList<>();
        try (AsyncProcessor<MyObject> processor = new AsyncProcessor<>(queueSize, new Consumer<MyObject>() {
            @Override
            public void accept(MyObject myObject) {
                //simulate some processing delay
                try {
                    Thread.sleep(10);
                    processed.add(myObject);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {

            // Enqueue objects to be processed
            long start = System.currentTimeMillis();
            for (int i = 0; i < nbObjects; i++) {
                processor.enqueue(new MyObject("Object " + i));
            }
            long duration = System.currentTimeMillis() - start;
            if (queueSize >= nbObjects) {
                assertTrue(duration < 10L); //queue size can accommodate all objects, enqueuing should be instantaneous
            } else {
                assertTrue(duration > (10L * (nbObjects / queueSize)));
            }

            if (!testClose) {
                // Wait a bit to let events process
                Thread.sleep(2000);

                assertProcessed(nbObjects, processed);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertProcessed(nbObjects, processed);
    }

    private static void assertProcessed(int nbObjects, List<MyObject> processed) {
        assertEquals(nbObjects, processed.size());

        for (int i = 0; i < nbObjects; i++) {
            assertEquals("Object " + i, processed.get(i).value);
        }
    }
}
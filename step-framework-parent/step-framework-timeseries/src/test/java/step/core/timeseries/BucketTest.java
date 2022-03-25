package step.core.timeseries;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class BucketTest {

    @Test
    public void getDistribution() {
        Bucket bucket = new Bucket();
        ConcurrentHashMap<Long, Long> distribution = new ConcurrentHashMap<>();

        bucket.setCount(20);
        bucket.setDistribution(distribution);

        distribution.put(1L, 2L);
        distribution.put(11L, 3L);
        distribution.put(15L, 1L);
        distribution.put(20L, 5L);
        distribution.put(21L, 4L);
        distribution.put(22L, 1L);
        distribution.put(25L, 4L);

        assertEquals(1L, bucket.getPercentile(0));
        assertEquals(1L, bucket.getPercentile(1));
        assertEquals(20L, bucket.getPercentile(50));
        assertEquals(25L, bucket.getPercentile(100));
    }
}
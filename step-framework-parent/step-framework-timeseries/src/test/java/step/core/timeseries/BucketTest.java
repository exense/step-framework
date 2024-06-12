package step.core.timeseries;

import org.junit.Test;
import step.core.timeseries.bucket.Bucket;

import java.util.TreeMap;
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
    
    @Test
    public void getDistributionWithDecimals() {
        Bucket bucket = new Bucket();
        TreeMap<Long, Long> distribution = new TreeMap<>();
        int count = 100;
        bucket.setCount(count);
        bucket.setDistribution(distribution);
        
        for (int i = 0; i < count; i+=2) {
            distribution.put(i * 10L, 2L); // all will be above 100
        }

        assertEquals(80, bucket.getPercentile(9.5));
        assertEquals(80, bucket.getPercentile(10));
        assertEquals(100, bucket.getPercentile(10.5));
        assertEquals(480L, bucket.getPercentile(50));
    }
}

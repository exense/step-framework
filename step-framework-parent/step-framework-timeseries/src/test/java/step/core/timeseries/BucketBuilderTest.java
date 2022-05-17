package step.core.timeseries;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Map;

public class BucketBuilderTest {

    @Test
    public void ingest1Point() {
        Map<String, Object> attributes = Map.of("key", "value");
        Bucket bucket = BucketBuilder.create(1000L).withAttributes(attributes).ingest(5L).build();
        assertEquals(1000L, bucket.getBegin());
        assertEquals(1L, bucket.getCount());
        assertEquals(5L, bucket.getSum());
        assertEquals(10L, bucket.getPclPrecision());
        assertEquals(5L, bucket.getMin());
        assertEquals(5L, bucket.getMax());
        assertEquals(attributes, bucket.getAttributes());
    }

    @Test
    public void ingest2Point() {
        Bucket bucket = BucketBuilder.create(0L).ingest(-5L).ingest(5L).build();
        assertEquals(0L, bucket.getBegin());
        assertEquals(2L, bucket.getCount());
        assertEquals(0L, bucket.getSum());
        assertEquals(10L, bucket.getPclPrecision());
        assertEquals(-5L, bucket.getMin());
        assertEquals(5L, bucket.getMax());
    }

    @Test
    public void accumulate() {
        Bucket bucket1 = BucketBuilder.create(0L).ingest(-5L).build();
        Bucket bucket2 = BucketBuilder.create(0L).ingest(5L).build();
        Bucket bucket = BucketBuilder.create(0L).accumulate(bucket1).accumulate(bucket2).build();
        assertEquals(0L, bucket.getBegin());
        assertEquals(2L, bucket.getCount());
        assertEquals(0L, bucket.getSum());
        assertEquals(10L, bucket.getPclPrecision());
        assertEquals(-5L, bucket.getMin());
        assertEquals(5L, bucket.getMax());
    }
}

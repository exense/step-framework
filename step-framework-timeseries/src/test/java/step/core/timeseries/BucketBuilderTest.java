package step.core.timeseries;

import static org.junit.Assert.*;
import org.junit.Test;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BucketBuilderTest {

    @Test
    public void ingest1Point() {
        BucketAttributes attributes = new BucketAttributes(Map.of("key", "value"));
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

    @Test
    public void accumulateWithAttributes() {
        BucketAttributes attributes = new BucketAttributes(Map.of("key", "value1"));
        Bucket bucket1 = BucketBuilder.create(0L).withAttributes(attributes).ingest(-5L).build();
        attributes = new BucketAttributes(Map.of("key", "value2"));
        Bucket bucket2 = BucketBuilder.create(0L).withAttributes(attributes).ingest(5L).build();
        attributes = new BucketAttributes(Map.of("key", "value3"));
        Bucket bucket3 = BucketBuilder.create(0L).withAttributes(attributes).ingest(5L).build();
        Bucket bucket = BucketBuilder.create(0L).withAccumulateAttributes(Set.of("key"), 2)
                .accumulate(bucket1)
                .accumulate(bucket2)
                .accumulate(bucket3).build();
        assertEquals(0L, bucket.getBegin());
        assertEquals(3L, bucket.getCount());
        assertEquals(5L, bucket.getSum());
        assertEquals(10L, bucket.getPclPrecision());
        assertEquals(-5L, bucket.getMin());
        assertEquals(5L, bucket.getMax());
        assertEquals(2, ((Set) bucket.getAttributes().get("key")).size());
        assertTrue(((Set) bucket.getAttributes().get("key")).containsAll(List.of("value1","value2")));
    }
}

package step.core.timeseries;

import static org.junit.Assert.*;

import org.junit.Test;
import step.core.timeseries.bucket.Aggregation;
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
    public void merge() {
        Bucket bucket1 = BucketBuilder.create(0L).ingest(-5L).build();
        Bucket bucket2 = BucketBuilder.create(0L).ingest(5L).build();
        Bucket bucket = BucketBuilder.create(0L).merge(bucket1).merge(bucket2).build();
        assertEquals(0L, bucket.getBegin());
        assertEquals(2L, bucket.getCount());
        assertEquals(0L, bucket.getSum());
        assertEquals(10L, bucket.getPclPrecision());
        assertEquals(-5L, bucket.getMin());
        assertEquals(5L, bucket.getMax());
    }

    @Test
    public void mergeWithAttributes() {
        BucketAttributes attributes = new BucketAttributes(Map.of("key", "value1"));
        Bucket bucket1 = BucketBuilder.create(0L).withAttributes(attributes).ingest(-5L).build();
        attributes = new BucketAttributes(Map.of("key", "value2"));
        Bucket bucket2 = BucketBuilder.create(0L).withAttributes(attributes).ingest(5L).build();
        attributes = new BucketAttributes(Map.of("key", "value3"));
        Bucket bucket3 = BucketBuilder.create(0L).withAttributes(attributes).ingest(5L).build();
        Bucket bucket = BucketBuilder.create(0L).withAccumulateAttributes(Set.of("key"), 2)
            .merge(bucket1)
            .merge(bucket2)
            .merge(bucket3).build();
        assertEquals(0L, bucket.getBegin());
        assertEquals(3L, bucket.getCount());
        assertEquals(5L, bucket.getSum());
        assertEquals(10L, bucket.getPclPrecision());
        assertEquals(-5L, bucket.getMin());
        assertEquals(5L, bucket.getMax());
        assertEquals(2, ((Set) bucket.getAttributes().get("key")).size());
        assertTrue(((Set) bucket.getAttributes().get("key")).containsAll(List.of("value1", "value2")));
    }

    /**
     * The samples are averaged over the number of sampling intervals the window covers, the intervals holding no
     * sample counting as zero.
     */
    @Test
    public void sampledAverage() {
        BucketBuilder builder = new BucketBuilder(Aggregation.SAMPLED_AVG, 0L, 60_000L).withSamplingInterval(15_000L);
        // 2 samples of 10 out of the 4 the window expects
        builder.ingest(10L).ingest(10L);
        assertEquals(5, builder.getScalarValue(), 0);
        assertEquals(10, builder.getAverageAsDouble(), 0);
    }

    /**
     * A window which isn't a whole number of sampling intervals expects no defined number of samples, the sampled
     * average then amounts to the plain one. The same applies to a builder without window.
     */
    @Test
    public void sampledAverageOfAWindowWhichIsNotAWholeNumberOfSamplingIntervals() {
        BucketBuilder builder = new BucketBuilder(Aggregation.SAMPLED_AVG, 0L, 35_000L).withSamplingInterval(15_000L);
        builder.ingest(10L).ingest(10L);
        assertEquals(10, builder.getScalarValue(), 0);

        BucketBuilder builderWithoutWindow = new BucketBuilder(Aggregation.SAMPLED_AVG, 0L).withSamplingInterval(15_000L);
        builderWithoutWindow.ingest(10L).ingest(10L);
        assertEquals(10, builderWithoutWindow.getScalarValue(), 0);
    }

    /**
     * An empty builder has no sample to average.
     */
    @Test
    public void sampledAverageOfAnEmptyBuilder() {
        BucketBuilder builder = new BucketBuilder(Aggregation.SAMPLED_AVG, 0L, 60_000L).withSamplingInterval(15_000L);
        assertEquals(0, builder.getScalarValue(), 0);
    }
}

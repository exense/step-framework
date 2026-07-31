package step.core.timeseries;

import org.junit.Test;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.ScalarBucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Covers the removal of the null-valued attributes performed by the ingestion pipeline.
 * <p>
 * Null-valued attributes are handled differently across collection implementations: persistent collections such as
 * MongoDB do not store them at all, whereas the buckets still held by the ingestion pipeline keep them. The ingestion
 * pipeline therefore drops them, so that both sides of {@link TimeSeriesCollection#queryTimeSeries} describe the
 * buckets of a same logical series with the exact same attributes.
 */
public class TimeSeriesIngestionNullAttributesTest extends TimeSeriesBaseTest {

    private static final long RESOLUTION = 1000;

    private static Map<String, Object> attributesWithNullValue() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "t1");
        attributes.put("nullAttribute", null);
        return attributes;
    }

    private static Bucket singlePersistedBucket(TimeSeriesCollection collection) {
        List<Bucket> buckets = collection.find(Filters.empty()).collect(Collectors.toList());
        assertEquals(1, buckets.size());
        return buckets.get(0);
    }

    /**
     * A point ingested with a null-valued attribute must be persisted without it.
     */
    @Test
    public void nullAttributesAreRemovedWhenIngestingPointsTest() {
        TimeSeriesCollection collection = getCollection(RESOLUTION);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        timeSeries.getIngestionPipeline().ingestPoint(attributesWithNullValue(), 1L, 10L);
        timeSeries.getIngestionPipeline().flush();

        assertEquals(new BucketAttributes(Map.of("name", "t1")), singlePersistedBucket(collection).getAttributes());
    }

    /**
     * A bucket ingested with a null-valued attribute must be persisted without it.
     */
    @Test
    public void nullAttributesAreRemovedWhenIngestingBucketsTest() {
        TimeSeriesCollection collection = getCollection(RESOLUTION);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        Bucket sourceBucket = getRandomBucket();
        sourceBucket.setBegin(1);
        sourceBucket.setAttributes(new BucketAttributes(attributesWithNullValue()));
        timeSeries.getIngestionPipeline().ingestBucket(sourceBucket);
        timeSeries.getIngestionPipeline().flush();

        assertEquals(new BucketAttributes(Map.of("name", "t1")), singlePersistedBucket(collection).getAttributes());
    }

    /**
     * The removal must already have been performed on the buckets still held by the ingestion pipeline, as these are
     * returned as they are by {@link TimeSeriesCollection#queryTimeSeries}.
     */
    @Test
    public void nullAttributesAreRemovedFromTheUnflushedBucketsTest() {
        TimeSeriesCollection collection = getCollection(RESOLUTION);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        timeSeries.getIngestionPipeline().ingestPoint(attributesWithNullValue(), 1L, 10L);

        InMemoryCollection<Bucket> unflushedBuckets = timeSeries.getIngestionPipeline().getCurrenStateToInMemoryCollection(Long.MAX_VALUE);
        List<Bucket> buckets = unflushedBuckets.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
        assertEquals(1, buckets.size());
        assertEquals(new BucketAttributes(Map.of("name", "t1")), buckets.get(0).getAttributes());
    }

    /**
     * Since the null-valued attributes are removed, an attribute set to null and an absent attribute describe the
     * same series and must therefore be accumulated into one single bucket.
     */
    @Test
    public void nullAndAbsentAttributesDescribeTheSameSeriesTest() {
        TimeSeriesCollection collection = getCollection(RESOLUTION);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        timeSeries.getIngestionPipeline().ingestPoint(attributesWithNullValue(), 1L, 10L);
        timeSeries.getIngestionPipeline().ingestPoint(Map.of("name", "t1"), 2L, 20L);
        timeSeries.getIngestionPipeline().flush();

        Bucket bucket = singlePersistedBucket(collection);
        assertEquals(new BucketAttributes(Map.of("name", "t1")), bucket.getAttributes());
        assertEquals(2, bucket.getCount());
        assertEquals(30, bucket.getSum());
    }

    /**
     * The null-valued attributes are removed on top of the ignored attributes of the collection. The remaining
     * attributes must be kept untouched, whatever their type.
     */
    @Test
    public void nullAndIgnoredAttributesAreBothRemovedTest() {
        TimeSeriesCollection collection = getCollection(RESOLUTION, Set.of("ignoredAttribute"));
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        Map<String, Object> attributes = attributesWithNullValue();
        attributes.put("ignoredAttribute", "ignoredValue");
        attributes.put("numericAttribute", 42);
        timeSeries.getIngestionPipeline().ingestPoint(attributes, 1L, 10L);
        timeSeries.getIngestionPipeline().flush();

        assertEquals(new BucketAttributes(Map.of("name", "t1", "numericAttribute", 42)),
            singlePersistedBucket(collection).getAttributes());
    }

    /**
     * The attributes provided by the caller must not be mutated by the removal, as they may be reused by the caller
     * or forwarded to the next pipeline.
     */
    @Test
    public void theProvidedAttributesAreNotMutatedTest() {
        TimeSeriesCollection collection = getCollection(RESOLUTION);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        Map<String, Object> pointAttributes = attributesWithNullValue();
        timeSeries.getIngestionPipeline().ingestPoint(pointAttributes, 1L, 10L);
        assertEquals(attributesWithNullValue(), pointAttributes);

        Bucket sourceBucket = getRandomBucket();
        sourceBucket.setBegin(1);
        sourceBucket.setAttributes(new BucketAttributes(attributesWithNullValue()));
        timeSeries.getIngestionPipeline().ingestBucket(sourceBucket);
        assertEquals(attributesWithNullValue(), sourceBucket.getAttributes());
    }

    /**
     * The regression this removal protects against: {@link TimeSeriesCollection#queryTimeSeries} concatenates the
     * buckets still held by the ingestion pipeline with the persisted ones. A persistent collection dropping the
     * null-valued attributes would otherwise report the two sides of a same logical series as two distinct source
     * series, which is observable as soon as one of the two aggregation axes reduces its series to a scalar.
     */
    @Test
    public void theFlushedAndUnflushedPartsOfASeriesAreOneSingleSeriesTest() {
        TimeSeriesCollection collection = new TimeSeriesCollection(new NullValuedAttributesDroppingCollection(), RESOLUTION);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();
        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();

        // The unflushed buckets are only queried for ranges ending after the last flush, hence the current time
        long now = System.currentTimeMillis();
        long rangeFrom = now - now % RESOLUTION;

        // First part of the series, flushed and therefore persisted without its null-valued attribute
        ingestionPipeline.ingestPoint(attributesWithNullValue(), now, 10L);
        ingestionPipeline.flush();
        // Second part, still held by the ingestion pipeline
        ingestionPipeline.ingestPoint(attributesWithNullValue(), now, 30L);

        // COUNT over the per-series scalars counts the source series of the group
        assertEquals(1, aggregate(timeSeries, rangeFrom, Aggregation.COUNT));
        // The whole series is reduced at once: sum(10, 30) and not the average of two distinct series
        assertEquals(40, aggregate(timeSeries, rangeFrom, Aggregation.AVG));
    }

    /**
     * Reduces every source series of the single time window starting at the given timestamp to its sum, and
     * aggregates the resulting per-series scalars with the given group-by aggregation.
     */
    private long aggregate(TimeSeries timeSeries, long rangeFrom, Aggregation groupAggregation) {
        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(
            new TimeSeriesAggregationQueryBuilder()
                .range(rangeFrom, rangeFrom + 2 * RESOLUTION)
                .window(2 * RESOLUTION)
                .withTimeAggregation(Aggregation.SUM)
                .groupBy(Set.of(), groupAggregation)
                .build());

        assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(1, series.size());
        Bucket bucket = series.values().iterator().next();
        assertEquals(ScalarBucket.class, bucket.getClass());
        return ((ScalarBucket) bucket).getValue();
    }

    /**
     * Mimics the persistent collections, such as MongoDB, which do not store the null-valued attributes.
     */
    private static class NullValuedAttributesDroppingCollection extends InMemoryCollection<Bucket> {

        @Override
        public Bucket save(Bucket entity) {
            BucketAttributes attributes = entity.getAttributes();
            if (attributes != null) {
                BucketAttributes persistedAttributes = new BucketAttributes(attributes);
                persistedAttributes.entrySet().removeIf(entry -> entry.getValue() == null);
                entity.setAttributes(persistedAttributes);
            }
            return super.save(entity);
        }
    }
}
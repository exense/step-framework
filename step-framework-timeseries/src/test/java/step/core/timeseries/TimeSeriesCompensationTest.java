package step.core.timeseries;

import org.junit.Test;
import step.core.collections.Filters;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Covers the compensation of ingested points, see {@link TimeSeriesIngestionPipeline#compensatePoint(Map, long, long)},
 * and the exclusion of the buckets left without any sample, see
 * {@link TimeSeriesAggregationConfig#setExcludeEmptyBuckets(boolean)}
 */
public class TimeSeriesCompensationTest extends TimeSeriesBaseTest {

    private static final long RESOLUTION = 1000;
    private static final Map<String, Object> SERIES_1 = Map.of("name", "t1");
    private static final Map<String, Object> SERIES_2 = Map.of("name", "t2");

    private TimeSeries newTimeSeries(boolean excludeEmptyBuckets, long... resolutions) {
        return new TimeSeriesBuilder()
            .registerCollections(Arrays.stream(resolutions).mapToObj(this::getCollection).collect(Collectors.toList()))
            .withAggregationConfig(new TimeSeriesAggregationConfig().setExcludeEmptyBuckets(excludeEmptyBuckets))
            .build();
    }

    /**
     * @return a time series holding one single time bucket with 2 series, the one of t1 being fully compensated
     */
    private TimeSeries newTimeSeriesWithOneCompensatedSeries(boolean excludeEmptyBuckets) {
        TimeSeries timeSeries = newTimeSeries(excludeEmptyBuckets, RESOLUTION);
        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
        ingestionPipeline.ingestPoint(SERIES_1, 1L, 100L);
        ingestionPipeline.ingestPoint(SERIES_2, 1L, 200L);
        ingestionPipeline.compensatePoint(SERIES_1, 1L, 100L);
        ingestionPipeline.flush();
        return timeSeries;
    }

    private TimeSeriesAggregationQueryBuilder singleBucketQuery() {
        return new TimeSeriesAggregationQueryBuilder()
            .range(0, RESOLUTION)
            .window(RESOLUTION);
    }

    private Bucket collectSingleBucket(TimeSeries timeSeries) {
        Map<BucketAttributes, Map<Long, Bucket>> series = collect(timeSeries, singleBucketQuery().build());
        assertEquals(1, series.size());
        Map<Long, Bucket> buckets = series.values().iterator().next();
        assertEquals(1, buckets.size());
        return buckets.values().iterator().next();
    }

    private Map<BucketAttributes, Map<Long, Bucket>> collect(TimeSeries timeSeries, TimeSeriesAggregationQuery query) {
        return timeSeries.getAggregationPipeline().collect(query).getSeries();
    }

    private Map<String, Bucket> collectByName(TimeSeries timeSeries, Aggregation timeAggregation) {
        TimeSeriesAggregationQuery query = singleBucketQuery()
            .withTimeAggregation(timeAggregation)
            .groupBy(Set.of("name"), Aggregation.MERGE)
            .build();
        return collect(timeSeries, query).entrySet().stream()
            .collect(Collectors.toMap(e -> (String) e.getKey().get("name"), e -> e.getValue().values().iterator().next()));
    }

    private List<Bucket> savedBuckets(TimeSeriesCollection collection) {
        return collection.find(Filters.empty()).collect(Collectors.toList());
    }

    @Test
    public void compensatedPointIsSubtractedFromTheQueuedBucket() {
        try (TimeSeries timeSeries = newTimeSeries(false, RESOLUTION)) {
            TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
            ingestionPipeline.ingestPoint(SERIES_1, 1L, 100L);
            ingestionPipeline.ingestPoint(SERIES_1, 2L, 230L);
            ingestionPipeline.compensatePoint(SERIES_1, 1L, 100L);

            Bucket bucket = collectSingleBucket(timeSeries);
            assertEquals(1, bucket.getCount());
            assertEquals(230, bucket.getSum());
            // The bracket of the compensated point is emptied, so that the percentiles are the ones of the point left
            assertEquals(Map.of(100L, 0L, 230L, 1L), bucket.getDistribution());
            assertEquals(230, bucket.getPercentile(50));
            // The min and the max can not be restored and keep including the compensated point
            assertEquals(100, bucket.getMin());
            assertEquals(230, bucket.getMax());
        }
    }

    @Test
    public void compensatedPointIsSubtractedOnceTheBucketIsSaved() {
        try (TimeSeries timeSeries = newTimeSeries(false, RESOLUTION)) {
            TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
            ingestionPipeline.ingestPoint(SERIES_1, 1L, 100L);
            ingestionPipeline.flush();
            ingestionPipeline.compensatePoint(SERIES_1, 1L, 100L);
            ingestionPipeline.flush();

            // The compensation is saved as an additional bucket of the same series and window
            List<Bucket> buckets = savedBuckets(timeSeries.getDefaultCollection());
            assertEquals(2, buckets.size());
            buckets.forEach(bucket -> assertEquals(0, bucket.getBegin()));
            Bucket bucket = collectSingleBucket(timeSeries);
            assertEquals(0, bucket.getCount());
            assertEquals(0, bucket.getSum());
        }
    }

    @Test
    public void compensatedPointIsSubtractedAtEveryResolution() {
        try (TimeSeries timeSeries = newTimeSeries(false, RESOLUTION, 2 * RESOLUTION, 4 * RESOLUTION)) {
            TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
            ingestionPipeline.ingestPoint(SERIES_1, 1L, 100L);
            ingestionPipeline.ingestPoint(SERIES_1, 2L, 230L);
            ingestionPipeline.compensatePoint(SERIES_1, 1L, 100L);
            timeSeries.getCollections().forEach(collection -> collection.getIngestionPipeline().flush());

            timeSeries.getCollections().forEach(collection -> {
                List<Bucket> buckets = savedBuckets(collection);
                assertEquals("Collection " + collection.getName(), 1, buckets.size());
                assertEquals("Collection " + collection.getName(), 1, buckets.get(0).getCount());
                assertEquals("Collection " + collection.getName(), 230, buckets.get(0).getSum());
            });
        }
    }

    @Test
    public void emptyBucketsAreExcludedWhenConfigured() {
        try (TimeSeries timeSeries = newTimeSeriesWithOneCompensatedSeries(true)) {
            // The series holding no sample anymore is reported as any other series without data, i.e. by its absence
            Map<String, Bucket> bucketsByName = collectByName(timeSeries, Aggregation.MERGE);
            assertEquals(Set.of("t2"), bucketsByName.keySet());
            assertEquals(1, bucketsByName.get("t2").getCount());
        }
    }

    @Test
    public void emptyBucketsAreReportedWhenNotConfigured() {
        try (TimeSeries timeSeries = newTimeSeriesWithOneCompensatedSeries(false)) {
            Map<String, Bucket> bucketsByName = collectByName(timeSeries, Aggregation.MERGE);
            assertEquals(Set.of("t1", "t2"), bucketsByName.keySet());
            assertEquals(0, bucketsByName.get("t1").getCount());
        }
    }

    @Test
    public void emptyBucketsAreExcludedForScalarAggregationsToo() {
        try (TimeSeries timeSeries = newTimeSeriesWithOneCompensatedSeries(true)) {
            Map<String, Bucket> bucketsByName = collectByName(timeSeries, Aggregation.AVG);
            assertEquals(Set.of("t2"), bucketsByName.keySet());
            assertEquals(200, bucketsByName.get("t2").getSum());
        }
        try (TimeSeries timeSeries = newTimeSeriesWithOneCompensatedSeries(false)) {
            // Without the exclusion, a scalar aggregate reports the empty series as one single sample of value 0,
            // see step.core.timeseries.bucket.ScalarBucket
            Map<String, Bucket> bucketsByName = collectByName(timeSeries, Aggregation.AVG);
            assertEquals(Set.of("t1", "t2"), bucketsByName.keySet());
            assertEquals(1, bucketsByName.get("t1").getCount());
            assertEquals(0, bucketsByName.get("t1").getSum());
        }
    }

    @Test
    public void partiallyCompensatedBucketsAreKept() {
        try (TimeSeries timeSeries = newTimeSeries(true, RESOLUTION)) {
            TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
            ingestionPipeline.ingestPoint(SERIES_1, 1L, 100L);
            ingestionPipeline.ingestPoint(SERIES_1, 2L, 230L);
            ingestionPipeline.compensatePoint(SERIES_1, 1L, 100L);
            ingestionPipeline.flush();

            Bucket bucket = collectSingleBucket(timeSeries);
            assertEquals(1, bucket.getCount());
            assertEquals(230, bucket.getSum());
        }
    }
}

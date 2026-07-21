package step.core.timeseries;

import org.junit.Test;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Covers the configurable group-by (series) aggregation of the {@link step.core.timeseries.aggregation.TimeSeriesAggregationPipeline}.
 * <p>
 * All tests are based on the same data set, ingested into a single time bucket, and made of 2 series:
 * <ul>
 *     <li>name=t1 : 10, 20        => count 2, sum 30,  min 10,  max 20</li>
 *     <li>name=t2 : 100, 200, 300 => count 3, sum 600, min 100, max 300</li>
 * </ul>
 */
public class TimeSeriesGroupByAggregationTest extends TimeSeriesBaseTest {

    private static final long RESOLUTION = 1000;

    private TimeSeries newTimeSeriesWith2Series() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 2L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 1L, 100L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 2L, 200L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 3L, 300L);
        }
        return timeSeries;
    }

    private Bucket collectSingleBucket(TimeSeries timeSeries, TimeSeriesAggregationQuery query) {
        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(query);
        assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(1, series.size());
        return series.values().iterator().next();
    }

    private TimeSeriesAggregationQueryBuilder singleBucketQuery() {
        return new TimeSeriesAggregationQueryBuilder()
            .range(0, RESOLUTION)
            .window(RESOLUTION);
    }

    /**
     * Without any explicit group-by aggregation, the pipeline must keep its historical behavior: AVG.
     */
    @Test
    public void defaultGroupByAggregationIsAvgTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        Bucket defaultBucket = collectSingleBucket(timeSeries, singleBucketQuery().build());
        Bucket avgBucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.AVG).build());

        assertEquals(avgBucket.getCount(), defaultBucket.getCount());
        assertEquals(avgBucket.getSum(), defaultBucket.getSum());
        assertEquals(avgBucket.getMin(), defaultBucket.getMin());
        assertEquals(avgBucket.getMax(), defaultBucket.getMax());
        assertEquals(avgBucket.getAverage(), defaultBucket.getAverage());
    }

    /**
     * AVG merges all the raw points of the aggregated series. The resulting bucket is therefore
     * strictly equivalent to a bucket built out of all the raw points: min/max/distribution are preserved.
     */
    @Test
    public void groupByAvgTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.AVG).build());

        // Each raw sample is one contributor
        assertEquals(5, bucket.getContributorCount());
        assertEquals(5, bucket.getCount());
        assertEquals(630, bucket.getSum());
        assertEquals(10, bucket.getMin());
        assertEquals(300, bucket.getMax());
        // The average is the average of all the raw points: 630 / 5
        assertEquals(126, bucket.getAverage());
        // The raw distribution is preserved, percentiles remain computable
        assertEquals(5, bucket.getDistribution().values().stream().mapToLong(Long::longValue).sum());
        assertEquals(300, bucket.getPercentile(100));
    }

    /**
     * SUM contributes the sum of each series, each series counting as one single contributor.
     */
    @Test
    public void groupBySumTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.SUM).build());

        // One contributor per aggregated series
        assertEquals(2, bucket.getContributorCount());
        // The raw sample count is preserved
        assertEquals(5, bucket.getCount());
        assertEquals(630, bucket.getSum());
        // The average is the average over the contributing series: 630 / 2
        assertEquals(315, bucket.getAverage());
    }

    /**
     * COUNT contributes the number of raw samples of each series, each series counting as one single contributor.
     */
    @Test
    public void groupByCountTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.COUNT).build());

        assertEquals(2, bucket.getContributorCount());
        assertEquals(5, bucket.getCount());
        // Sum of the counts of both series: 2 + 3
        assertEquals(5, bucket.getSum());
        // 5 / 2
        assertEquals(2, bucket.getAverage());
    }

    /**
     * Whatever the group-by aggregation, the distribution, the min and the max are merged by union, so that
     * percentiles remain percentiles over the raw samples and min/max remain the raw min/max.
     */
    @Test
    public void distributionMinAndMaxAreAlwaysMergedByUnionTest() {
        for (Aggregation aggregation : List.of(Aggregation.AVG, Aggregation.SUM, Aggregation.COUNT)) {
            TimeSeries timeSeries = newTimeSeriesWith2Series();
            Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
                .groupBy(Set.of(), aggregation).build());

            String message = "Aggregation " + aggregation;
            assertEquals(message, 10, bucket.getMin());
            assertEquals(message, 300, bucket.getMax());
            // The distribution holds all 5 raw samples
            assertEquals(message, 5, bucket.getDistribution().values().stream().mapToLong(Long::longValue).sum());
            assertEquals(message, 300, bucket.getPercentile(100));
            assertEquals(message, 10, bucket.getPercentile(0));
        }
    }

    /**
     * When each group contains a single series, the group-by aggregation must be a no-op
     * on the sum, whatever the configured aggregation.
     */
    @Test
    public void groupByDimensionWithSingleSeriesPerGroupTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        for (Aggregation aggregation : Set.of(Aggregation.AVG, Aggregation.SUM)) {
            TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
                .groupBy(Set.of("name"), aggregation).build());

            assertEquals(2, response.getSeries().size());

            Bucket t1 = response.getSeries().get(new BucketAttributes(Map.of("name", "t1"))).values().iterator().next();
            Bucket t2 = response.getSeries().get(new BucketAttributes(Map.of("name", "t2"))).values().iterator().next();

            assertEquals("Aggregation " + aggregation, 30, t1.getSum());
            assertEquals("Aggregation " + aggregation, 2, t1.getCount());
            assertEquals("Aggregation " + aggregation, 600, t2.getSum());
            assertEquals("Aggregation " + aggregation, 3, t2.getCount());
        }
    }

    /**
     * The group attributes must be reported on the resulting buckets.
     */
    @Test
    public void groupAttributesArePropagatedTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
            .groupBy(Set.of("name"), Aggregation.SUM).build());

        response.getSeries().forEach((key, series) ->
            series.values().forEach(bucket -> assertEquals(key, bucket.getAttributes())));
    }

    /**
     * The group-by aggregation must be applied per time bucket and must not leak across time buckets.
     */
    @Test
    public void groupByAggregationIsAppliedPerTimeBucketTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            // First time bucket [0, 1000)
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 1L, 20L);
            // Second time bucket [1000, 2000): only one series
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1001L, 100L);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(
            new TimeSeriesAggregationQueryBuilder()
                .range(0, 2 * RESOLUTION)
                .window(RESOLUTION)
                .groupBy(Set.of(), Aggregation.SUM)
                .build());

        assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(2, series.size());

        Bucket firstBucket = series.get(0L);
        assertEquals(2, firstBucket.getContributorCount());
        assertEquals(30, firstBucket.getSum());
        assertEquals(15, firstBucket.getAverage());

        Bucket secondBucket = series.get(RESOLUTION);
        assertEquals(1, secondBucket.getContributorCount());
        assertEquals(100, secondBucket.getSum());
        assertEquals(100, secondBucket.getAverage());
    }

    /**
     * The time-window aggregation must remain an accumulation of the raw points, independently of the
     * configured group-by aggregation: aggregating 2 source buckets of the same series into one time
     * window must not be counted as 2 contributors of the group-by aggregation.
     */
    @Test
    public void timeWindowAggregationIsIndependentOfGroupByAggregationTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            // Single series, spread over 2 source buckets, both collected into the same time window
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1001L, 20L);
        }

        Bucket bucket = collectSingleBucket(timeSeries, new TimeSeriesAggregationQueryBuilder()
            .range(0, 2 * RESOLUTION)
            .window(2 * RESOLUTION)
            .groupBy(Set.of(), Aggregation.SUM)
            .build());

        assertEquals(1, bucket.getContributorCount());
        assertEquals(2, bucket.getCount());
        assertEquals(30, bucket.getSum());
        assertEquals(30, bucket.getAverage());
    }

    // ------------------------------------------------------------------------------------------------------------
    // Time-window aggregation
    // ------------------------------------------------------------------------------------------------------------

    /**
     * One single series spread over 2 source buckets of unequal sample counts, both falling into the same time window:
     * <ul>
     *     <li>[0, 1000)    : 10, 30 => count 2, sum 40</li>
     *     <li>[1000, 2000) : 100    => count 1, sum 100</li>
     * </ul>
     */
    private TimeSeries newTimeSeriesWith2SourceBuckets() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 2L, 30L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1001L, 100L);
        }
        return timeSeries;
    }

    private TimeSeriesAggregationQueryBuilder singleWindowOver2SourceBucketsQuery() {
        return new TimeSeriesAggregationQueryBuilder()
            .range(0, 2 * RESOLUTION)
            .window(2 * RESOLUTION);
    }

    /**
     * Without any explicit time aggregation, the pipeline must keep its historical behavior: AVG.
     */
    @Test
    public void defaultTimeAggregationIsAvgTest() {
        Bucket defaultBucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().build());
        Bucket avgBucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.AVG).build());

        assertEquals(avgBucket.getContributorCount(), defaultBucket.getContributorCount());
        assertEquals(avgBucket.getCount(), defaultBucket.getCount());
        assertEquals(avgBucket.getSum(), defaultBucket.getSum());
        assertEquals(avgBucket.getAverage(), defaultBucket.getAverage());
    }

    /**
     * AVG over the time window merges all the raw samples of the successive source buckets.
     */
    @Test
    public void timeAggregationAvgTest() {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.AVG).build());

        // Each raw sample is one contributor
        assertEquals(3, bucket.getContributorCount());
        assertEquals(3, bucket.getCount());
        assertEquals(140, bucket.getSum());
        // Average over the raw samples: 140 / 3
        assertEquals(46, bucket.getAverage());
    }

    /**
     * SUM over the time window contributes the sum of each source bucket, each source bucket counting as one
     * single contributor. The average is therefore the mean sum per source interval.
     */
    @Test
    public void timeAggregationSumTest() {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.SUM).build());

        // One contributor per source bucket
        assertEquals(2, bucket.getContributorCount());
        // The raw sample count is preserved
        assertEquals(3, bucket.getCount());
        assertEquals(140, bucket.getSum());
        // 140 / 2
        assertEquals(70, bucket.getAverage());
    }

    /**
     * COUNT over the time window contributes the number of raw samples of each source bucket.
     */
    @Test
    public void timeAggregationCountTest() {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.COUNT).build());

        assertEquals(2, bucket.getContributorCount());
        assertEquals(3, bucket.getCount());
        // 2 + 1
        assertEquals(3, bucket.getSum());
        // 3 / 2
        assertEquals(1, bucket.getAverage());
    }

    /**
     * The distribution, min and max are merged by union on the time axis too.
     */
    @Test
    public void timeAggregationAlwaysMergesDistributionMinAndMaxByUnionTest() {
        for (Aggregation aggregation : List.of(Aggregation.AVG, Aggregation.SUM, Aggregation.COUNT)) {
            Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
                singleWindowOver2SourceBucketsQuery().withTimeAggregation(aggregation).build());

            String message = "Aggregation " + aggregation;
            assertEquals(message, 10, bucket.getMin());
            assertEquals(message, 100, bucket.getMax());
            assertEquals(message, 3, bucket.getDistribution().values().stream().mapToLong(Long::longValue).sum());
            assertEquals(message, 100, bucket.getPercentile(100));
            assertEquals(message, 10, bucket.getPercentile(0));
        }
    }

    /**
     * The two axes are independent and compose: the time aggregation is applied first, per series, and the group-by
     * aggregation is then applied on its result.
     */
    @Test
    public void timeAndGroupByAggregationsComposeTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            // t1 over 2 source buckets: [0,1000) => 10, 30 (sum 40) and [1000,2000) => 100 (sum 100)
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 2L, 30L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1001L, 100L);
            // t2 over 1 source bucket: [0,1000) => 5
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 1L, 5L);
        }

        Bucket bucket = collectSingleBucket(timeSeries, new TimeSeriesAggregationQueryBuilder()
            .range(0, 2 * RESOLUTION)
            .window(2 * RESOLUTION)
            .withTimeAggregation(Aggregation.SUM)
            .groupBy(Set.of(), Aggregation.SUM)
            .build());

        // Time axis: t1 => sum 140, t2 => sum 5. Group axis: one contributor per series
        assertEquals(2, bucket.getContributorCount());
        assertEquals(145, bucket.getSum());
        // The raw sample count survives both axes
        assertEquals(4, bucket.getCount());
        // 145 / 2
        assertEquals(72, bucket.getAverage());
        // Raw distribution survives both axes
        assertEquals(4, bucket.getDistribution().values().stream().mapToLong(Long::longValue).sum());
        assertEquals(5, bucket.getMin());
        assertEquals(100, bucket.getMax());
    }

    // ------------------------------------------------------------------------------------------------------------
    // Attribute collection
    // ------------------------------------------------------------------------------------------------------------

    /**
     * Attribute collection must remain functional and must be applied on the attributes of the aggregated series.
     */
    @Test
    public void attributeCollectionTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t3", "status", "FAILED"), 1L, 30L);
        }

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .withAttributeCollection(Set.of("status"), 10)
            .build());

        assertEquals(Set.of("PASSED", "FAILED"), bucket.getAttributes().get("status"));
    }

    /**
     * The number of collected values per key must be capped by the configured limit.
     */
    @Test
    public void attributeCollectionLimitTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 1L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t3"), 1L, 30L);
        }

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .withAttributeCollection(Set.of("name"), 2)
            .build());

        assertEquals(2, ((Set<?>) bucket.getAttributes().get("name")).size());
    }

    /**
     * Attribute collection must work alongside grouping: the group attributes and the collected attributes
     * both end up on the resulting bucket.
     */
    @Test
    public void attributeCollectionWithGroupingTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 1L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 30L);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
            .groupBy(Set.of("name"), Aggregation.SUM)
            .withAttributeCollection(Set.of("status"), 10)
            .build());

        assertEquals(2, response.getSeries().size());

        Bucket t1 = response.getSeries().get(new BucketAttributes(Map.of("name", "t1"))).values().iterator().next();
        assertEquals("t1", t1.getAttributes().get("name"));
        assertEquals(Set.of("PASSED", "FAILED"), t1.getAttributes().get("status"));

        Bucket t2 = response.getSeries().get(new BucketAttributes(Map.of("name", "t2"))).values().iterator().next();
        assertEquals("t2", t2.getAttributes().get("name"));
        assertEquals(Set.of("PASSED"), t2.getAttributes().get("status"));
    }

    /**
     * Collecting an attribute which is also a group dimension must keep the exact group value and must not
     * corrupt the response by replacing it with a set.
     */
    @Test
    public void attributeCollectionOnGroupDimensionTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 1L, 20L);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
            .groupBy(Set.of("name"), Aggregation.AVG)
            .withAttributeCollection(Set.of("name"), 10)
            .build());

        assertEquals(2, response.getSeries().size());
        response.getSeries().forEach((key, series) ->
            series.values().forEach(bucket -> assertEquals(key.get("name"), bucket.getAttributes().get("name"))));
    }

    /**
     * Collecting the attributes must not mutate the group attributes used as key of the response map.
     */
    @Test
    public void attributeCollectionDoesNotMutateGroupKeyTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 1L, 20L);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
            .groupBy(Set.of("name"), Aggregation.AVG)
            .withAttributeCollection(Set.of("status"), 10)
            .build());

        // The key must remain the plain group attributes, without any collected attribute
        assertEquals(Set.of(new BucketAttributes(Map.of("name", "t1"))), response.getSeries().keySet());
    }
}
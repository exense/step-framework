package step.core.timeseries;

import org.junit.Test;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.ScalarBucket;
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

    private ScalarBucket collectSingleScalarBucket(TimeSeries timeSeries, TimeSeriesAggregationQuery query) {
        Bucket bucket = collectSingleBucket(timeSeries, query);
        assertEquals(ScalarBucket.class, bucket.getClass());
        return (ScalarBucket) bucket;
    }

    /**
     * Without any explicit aggregation, both axes must merge, i.e. keep the historical behavior of the pipeline.
     */
    @Test
    public void defaultAggregationsAreMergeTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        Bucket defaultBucket = collectSingleBucket(timeSeries, singleBucketQuery().build());
        Bucket mergeBucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .withTimeAggregation(Aggregation.MERGE)
            .groupBy(Set.of(), Aggregation.MERGE).build());

        assertEquals(Bucket.class, defaultBucket.getClass());
        assertEquals(mergeBucket.getCount(), defaultBucket.getCount());
        assertEquals(mergeBucket.getSum(), defaultBucket.getSum());
        assertEquals(mergeBucket.getMin(), defaultBucket.getMin());
        assertEquals(mergeBucket.getMax(), defaultBucket.getMax());
        assertEquals(mergeBucket.getAverage(), defaultBucket.getAverage());
    }

    /**
     * MERGE merges all the raw points of the aggregated series. The resulting bucket is therefore
     * strictly equivalent to a bucket built out of all the raw points: min/max/distribution are preserved.
     */
    @Test
    public void groupByMergeTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.MERGE).build());

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
     * AVG reduces the merged series to the average of their raw points.
     */
    @Test
    public void groupByAvgTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        ScalarBucket bucket = collectSingleScalarBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.AVG).build());

        // 630 / 5
        assertEquals(126, bucket.getValue());
    }

    /**
     * SUM reduces the merged series to the sum of their raw points.
     */
    @Test
    public void groupBySumTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        ScalarBucket bucket = collectSingleScalarBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.SUM).build());

        assertEquals(630, bucket.getValue());
    }

    /**
     * COUNT reduces the merged series to their number of raw points. The time aggregation being MERGE, the group
     * receives the raw points of every series, hence the total number of raw points and not the number of series.
     */
    @Test
    public void groupByCountTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        ScalarBucket bucket = collectSingleScalarBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.COUNT).build());

        // 2 + 3 raw points
        assertEquals(5, bucket.getValue());
    }

    /**
     * MIN reduces the merged series to the lowest of their raw points.
     */
    @Test
    public void groupByMinTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        ScalarBucket bucket = collectSingleScalarBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.MIN).build());

        // min(10, 100)
        assertEquals(10, bucket.getValue());
    }

    /**
     * MAX reduces the merged series to the highest of their raw points.
     */
    @Test
    public void groupByMaxTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        ScalarBucket bucket = collectSingleScalarBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.MAX).build());

        // max(20, 300)
        assertEquals(300, bucket.getValue());
    }

    /**
     * A scalar aggregate is equivalent to a bucket holding one single sample: the inherited accessors of a
     * {@link ScalarBucket} must all report the scalar, so that the consumers reading the generic bucket fields
     * don't silently read an empty bucket.
     */
    @Test
    public void scalarBucketsReportTheirValueOnTheInheritedFieldsTest() {
        for (Aggregation aggregation : List.of(Aggregation.AVG, Aggregation.SUM, Aggregation.COUNT, Aggregation.MIN, Aggregation.MAX)) {
            TimeSeries timeSeries = newTimeSeriesWith2Series();
            ScalarBucket bucket = collectSingleScalarBucket(timeSeries, singleBucketQuery()
                .groupBy(Set.of(), aggregation).build());

            String message = "Aggregation " + aggregation;
            long value = bucket.getValue();
            assertEquals(message, 1, bucket.getCount());
            assertEquals(message, value, bucket.getSum());
            assertEquals(message, value, bucket.getMin());
            assertEquals(message, value, bucket.getMax());
            assertEquals(message, value, bucket.getAverage());
            assertEquals(message, value, bucket.getPercentile(50));
            assertEquals(message, Map.of(value, 1L), bucket.getDistribution());
        }
    }

    /**
     * When merging, the distribution, the min and the max are merged by union, so that percentiles remain
     * percentiles over the raw samples and min/max remain the raw min/max.
     */
    @Test
    public void groupByMergeKeepsTheRawDistributionTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();
        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .groupBy(Set.of(), Aggregation.MERGE).build());

        assertEquals(10, bucket.getMin());
        assertEquals(300, bucket.getMax());
        // The distribution holds all 5 raw samples
        assertEquals(5, bucket.getDistribution().values().stream().mapToLong(Long::longValue).sum());
        assertEquals(300, bucket.getPercentile(100));
        assertEquals(10, bucket.getPercentile(0));
    }

    /**
     * When each group contains a single series, the group-by aggregation reduces that series alone.
     */
    @Test
    public void groupByDimensionWithSingleSeriesPerGroupTest() {
        TimeSeries timeSeries = newTimeSeriesWith2Series();

        TimeSeriesAggregationResponse sums = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
            .groupBy(Set.of("name"), Aggregation.SUM).build());
        assertEquals(2, sums.getSeries().size());
        assertEquals(30, scalar(sums, "t1").getValue());
        assertEquals(600, scalar(sums, "t2").getValue());

        TimeSeriesAggregationResponse averages = timeSeries.getAggregationPipeline().collect(singleBucketQuery()
            .groupBy(Set.of("name"), Aggregation.AVG).build());
        assertEquals(2, averages.getSeries().size());
        // 30 / 2 and 600 / 3
        assertEquals(15, scalar(averages, "t1").getValue());
        assertEquals(200, scalar(averages, "t2").getValue());
    }

    private static ScalarBucket scalar(TimeSeriesAggregationResponse response, String name) {
        Bucket bucket = response.getSeries().get(new BucketAttributes(Map.of("name", name))).values().iterator().next();
        assertEquals(ScalarBucket.class, bucket.getClass());
        return (ScalarBucket) bucket;
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

        // The first time bucket holds both series, the second one only t1
        assertEquals(30, ((ScalarBucket) series.get(0L)).getValue());
        assertEquals(100, ((ScalarBucket) series.get(RESOLUTION)).getValue());
    }

    /**
     * The time-window aggregation must remain an accumulation of the raw points, independently of the
     * configured group-by aggregation: merging 2 source buckets of the same series into one time
     * window must not be counted as 2 contributions of the group-by aggregation.
     */
    @Test
    public void timeWindowAggregationIsIndependentOfGroupByAggregationTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            // Single series, spread over 2 source buckets, both collected into the same time window
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1001L, 20L);
        }

        ScalarBucket bucket = collectSingleScalarBucket(timeSeries, new TimeSeriesAggregationQueryBuilder()
            .range(0, 2 * RESOLUTION)
            .window(2 * RESOLUTION)
            .groupBy(Set.of(), Aggregation.SUM)
            .build());

        assertEquals(30, bucket.getValue());
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
     * Reduces the single series of the data set with the given time aggregation. The group axis merges, so the
     * scalar of the series ends up as the one single sample of the resulting bucket.
     */
    private long timeAggregate(Aggregation timeAggregation) {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(timeAggregation).build());

        assertEquals("Aggregation " + timeAggregation, 1, bucket.getCount());
        assertEquals("Aggregation " + timeAggregation, bucket.getSum(), bucket.getAverage());
        return bucket.getSum();
    }

    /**
     * Without any explicit time aggregation, the pipeline must keep its historical behavior: MERGE.
     */
    @Test
    public void defaultTimeAggregationIsMergeTest() {
        Bucket defaultBucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().build());
        Bucket mergeBucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.MERGE).build());

        assertEquals(mergeBucket.getCount(), defaultBucket.getCount());
        assertEquals(mergeBucket.getSum(), defaultBucket.getSum());
        assertEquals(mergeBucket.getAverage(), defaultBucket.getAverage());
    }

    /**
     * MERGE over the time window keeps all the raw samples of the successive source buckets.
     */
    @Test
    public void timeAggregationMergeTest() {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.MERGE).build());

        assertEquals(3, bucket.getCount());
        assertEquals(140, bucket.getSum());
        // Average over the raw samples: 140 / 3
        assertEquals(46, bucket.getAverage());
    }

    /**
     * AVG over the time window reduces the merged source buckets to the average of their raw samples.
     */
    @Test
    public void timeAggregationAvgTest() {
        // 140 / 3
        assertEquals(46, timeAggregate(Aggregation.AVG));
    }

    /**
     * SUM over the time window reduces the merged source buckets to the sum of their raw samples.
     */
    @Test
    public void timeAggregationSumTest() {
        // 40 + 100
        assertEquals(140, timeAggregate(Aggregation.SUM));
    }

    /**
     * COUNT over the time window reduces the merged source buckets to their number of raw samples.
     */
    @Test
    public void timeAggregationCountTest() {
        // 2 + 1
        assertEquals(3, timeAggregate(Aggregation.COUNT));
    }

    /**
     * MIN over the time window keeps the lowest raw sample, whatever the sample counts of the source buckets.
     */
    @Test
    public void timeAggregationMinTest() {
        // min(10, 100)
        assertEquals(10, timeAggregate(Aggregation.MIN));
    }

    /**
     * MAX over the time window keeps the highest raw sample.
     */
    @Test
    public void timeAggregationMaxTest() {
        // max(30, 100)
        assertEquals(100, timeAggregate(Aggregation.MAX));
    }

    /**
     * The distribution, min and max are merged by union on the time axis too.
     */
    @Test
    public void timeAggregationMergeKeepsTheRawDistributionTest() {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SourceBuckets(),
            singleWindowOver2SourceBucketsQuery().withTimeAggregation(Aggregation.MERGE).build());

        assertEquals(10, bucket.getMin());
        assertEquals(100, bucket.getMax());
        assertEquals(3, bucket.getDistribution().values().stream().mapToLong(Long::longValue).sum());
        assertEquals(100, bucket.getPercentile(100));
        assertEquals(10, bucket.getPercentile(0));
    }

    /**
     * 2 series spread over the 2 source buckets of one single time window:
     * <ul>
     *     <li>name=t1 : [0,1000) => 10, 30 and [1000,2000) => 100 => count 3, sum 140, min 10, max 100</li>
     *     <li>name=t2 : [0,1000) => 5                            => count 1, sum 5,   min 5,  max 5</li>
     * </ul>
     */
    private TimeSeries newTimeSeriesWith2SeriesOver2SourceBuckets() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 2L, 30L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1"), 1001L, 100L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2"), 1L, 5L);
        }
        return timeSeries;
    }

    private TimeSeriesAggregationQueryBuilder singleWindowOver2SeriesQuery() {
        return new TimeSeriesAggregationQueryBuilder()
            .range(0, 2 * RESOLUTION)
            .window(2 * RESOLUTION);
    }

    /**
     * The two axes are independent and compose: the time aggregation reduces each series first, and the group-by
     * aggregation is then applied on the resulting per-series scalars.
     */
    @Test
    public void timeAndGroupByAggregationsComposeTest() {
        ScalarBucket bucket = collectSingleScalarBucket(newTimeSeriesWith2SeriesOver2SourceBuckets(),
            singleWindowOver2SeriesQuery()
                .withTimeAggregation(Aggregation.SUM)
                .groupBy(Set.of(), Aggregation.SUM)
                .build());

        // Time axis: t1 => sum 140, t2 => sum 5. Group axis: the sum of both
        assertEquals(145, bucket.getValue());
    }

    /**
     * The group-by aggregation is applied on the per-series scalars, not on the raw samples: summing the minima of
     * the series is not the same as the minimum of all the raw samples.
     */
    @Test
    public void timeMinAndGroupBySumComposeTest() {
        ScalarBucket bucket = collectSingleScalarBucket(newTimeSeriesWith2SeriesOver2SourceBuckets(),
            singleWindowOver2SeriesQuery()
                .withTimeAggregation(Aggregation.MIN)
                .groupBy(Set.of(), Aggregation.SUM)
                .build());

        // Time axis: t1 => min 10, t2 => min 5. Group axis: the sum of both minima
        assertEquals(15, bucket.getValue());
    }

    /**
     * A scalar time aggregation followed by a MERGE group aggregation yields a bucket holding one sample per series,
     * i.e. a bucket over the per-series scalars rather than over the raw samples.
     */
    @Test
    public void scalarTimeAggregationFollowedByMergeTest() {
        Bucket bucket = collectSingleBucket(newTimeSeriesWith2SeriesOver2SourceBuckets(),
            singleWindowOver2SeriesQuery()
                .withTimeAggregation(Aggregation.SUM)
                .groupBy(Set.of(), Aggregation.MERGE)
                .build());

        // One sample per series: 140 and 5
        assertEquals(2, bucket.getCount());
        assertEquals(145, bucket.getSum());
        assertEquals(5, bucket.getMin());
        assertEquals(140, bucket.getMax());
        assertEquals(72, bucket.getAverage());
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
     * Attribute collection must remain functional when the time aggregation reduces the series to a scalar: the
     * series contributes its scalar value, but its attributes must still be collected.
     */
    @Test
    public void attributeCollectionWithScalarTimeAggregationTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t3", "status", "FAILED"), 1L, 30L);
        }

        Bucket bucket = collectSingleBucket(timeSeries, singleBucketQuery()
            .withTimeAggregation(Aggregation.SUM)
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

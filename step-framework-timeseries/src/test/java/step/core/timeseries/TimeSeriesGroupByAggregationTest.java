package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;
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
import static step.core.timeseries.aggregation.TimeSeriesAggregationPipeline.calculateAlignmentResolution;

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
     * When both axes merge, the pipeline merges the source buckets directly into their resulting bucket, without
     * materializing the series they belong to. The partitioning by group and by time bucket, as well as the
     * attribute collection, must be preserved.
     */
    @Test
    public void groupByMergeOverSeveralGroupsAndTimeBucketsTest() {
        TimeSeries timeSeries = getNewTimeSeries(RESOLUTION);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            // Group t1, first time bucket: 2 series
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 20L);
            // Group t1, second time bucket
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1001L, 30L);
            // Group t2, first time bucket
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 100L);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(
            new TimeSeriesAggregationQueryBuilder()
                .range(0, 2 * RESOLUTION)
                .window(RESOLUTION)
                .groupBy(Set.of("name"), Aggregation.MERGE)
                .withAttributeCollection(Set.of("status"), 10)
                .build());

        assertEquals(2, response.getSeries().size());

        Map<Long, Bucket> t1 = response.getSeries().get(new BucketAttributes(Map.of("name", "t1")));
        assertEquals(2, t1.size());
        // Both series of the group are merged into the first time bucket
        Bucket t1FirstBucket = t1.get(0L);
        assertEquals(2, t1FirstBucket.getCount());
        assertEquals(30, t1FirstBucket.getSum());
        assertEquals(10, t1FirstBucket.getMin());
        assertEquals(20, t1FirstBucket.getMax());
        assertEquals("t1", t1FirstBucket.getAttributes().get("name"));
        assertEquals(Set.of("PASSED", "FAILED"), t1FirstBucket.getAttributes().get("status"));
        // The second time bucket holds the later point only
        Bucket t1SecondBucket = t1.get(RESOLUTION);
        assertEquals(1, t1SecondBucket.getCount());
        assertEquals(30, t1SecondBucket.getSum());
        assertEquals(Set.of("PASSED"), t1SecondBucket.getAttributes().get("status"));

        Map<Long, Bucket> t2 = response.getSeries().get(new BucketAttributes(Map.of("name", "t2")));
        assertEquals(1, t2.size());
        assertEquals(100, t2.get(0L).getSum());
        assertEquals("t2", t2.get(0L).getAttributes().get("name"));
    }


    // ------------------------------------------------------------------------------------------------------------
    // Alignment grid
    // ------------------------------------------------------------------------------------------------------------

    private TimeSeries newTimeSeries(long resolution, int maxAlignmentIntervals) {
        TimeSeriesCollection collection = new TimeSeriesCollection(new InMemoryCollection<>(), resolution);
        return new TimeSeriesBuilder()
            .registerCollection(collection)
            .withAggregationConfig(new TimeSeriesAggregationConfig().setMaxAlignmentIntervals(maxAlignmentIntervals))
            .build();
    }

    /**
     * A group (name=t1) made of 2 series with different lifetimes, plus a second group, spread over 4 source buckets:
     * <ul>
     *     <li>[0,1000)    : t1/PASSED 10, t1/FAILED 20, t2/PASSED 100</li>
     *     <li>[1000,2000) : t1/PASSED 30</li>
     *     <li>[2000,3000) : t1/PASSED 20</li>
     *     <li>[3000,4000) : t1/PASSED 20</li>
     * </ul>
     * The total of the group t1 is therefore 30, 30, 20 and 20 over the 4 source buckets it spans.
     */
    private TimeSeries newTimeSeriesWithSeriesOfDifferentLifetimes(int maxAlignmentIntervals) {
        TimeSeries timeSeries = newTimeSeries(RESOLUTION, maxAlignmentIntervals);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            // This series only exists during the first source bucket
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 100L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1001L, 30L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 2001L, 20L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 3001L, 20L);
        }
        return timeSeries;
    }

    private TimeSeriesAggregationQueryBuilder avgOverSumQuery() {
        return new TimeSeriesAggregationQueryBuilder()
            .range(0, 10 * RESOLUTION)
            .withTimeAggregation(Aggregation.AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .withAttributeCollection(Set.of("status"), 10);
    }

    private static long valueOf(TimeSeriesAggregationResponse response, String name, long bucketIndex) {
        return response.getSeries().get(new BucketAttributes(Map.of("name", name))).get(bucketIndex).getSum();
    }

    private static long alignmentResolution(long sourceResolution, long resultResolution, long rangeDiff, long maxAlignmentIntervals) {
        return alignmentResolution(Aggregation.AVG, Aggregation.SUM, sourceResolution, resultResolution, rangeDiff, maxAlignmentIntervals);
    }

    private static long alignmentResolution(Aggregation timeAggregation, Aggregation groupAggregation, long sourceResolution,
                                            long resultResolution, long rangeDiff, long maxAlignmentIntervals) {
        return calculateAlignmentResolution(new TimeSeriesAggregationQueryBuilder()
                .withTimeAggregation(timeAggregation)
                .groupBy(Set.of(), groupAggregation)
                .build(),
            sourceResolution, resultResolution, rangeDiff, maxAlignmentIntervals);
    }

    /**
     * The ideal alignment grid is the source resolution, i.e. the finest grid the stored data allows.
     */
    @Test
    public void alignmentResolutionIsTheSourceResolutionWhenTheBudgetAllowsTest() {
        // 1 hour shrunk into one single response bucket, over a 30 seconds collection: 120 intervals, well below the cap
        assertEquals(30_000, alignmentResolution(30_000, 3_600_000, 3_600_000, 500));
        // Same range split into 60 response buckets of 1 minute, over a 5 seconds collection: 720 intervals
        assertEquals(5_000, alignmentResolution(5_000, 60_000, 3_600_000, 1000));
    }

    /**
     * The number of alignment intervals over the whole range is bounded, so that the number of builders the pipeline
     * retains while collecting stays bounded too. The grid is coarsened just enough to fit the budget.
     */
    @Test
    public void alignmentResolutionIsCappedByTheBudgetTest() {
        // 1 hour split into 120 response buckets of 30 seconds, over a 5 seconds collection
        // alignment interval of 10 seconds expected, giving for the 1 hour interval 360 alignment intervals (below
        // the 500 cap), while the next lower interval of 5 seconds would be over the limit
        assertEquals(10_000, alignmentResolution(5_000, 30_000, 3_600_000, 500));
        // The same query with a larger budget affords the source resolution: 720 intervals
        assertEquals(5_000, alignmentResolution(5_000, 30_000, 3_600_000, 1000));
        // With a smaller budget the grid is coarsened further: 15 seconds, i.e. 240 intervals
        assertEquals(15_000, alignmentResolution(5_000, 30_000, 3_600_000, 300));
        // And with a budget the response buckets alone exhaust, no alignment applies at all
        assertEquals(30_000, alignmentResolution(5_000, 30_000, 3_600_000, 200));
    }

    /**
     * A response finer than the budget gets no alignment at all: its response buckets are already close to the source
     * resolution, so there is little to correct, and it is also the response which retains the most builders.
     */
    @Test
    public void noAlignmentWhenTheResponseBucketsExhaustTheBudgetTest() {
        // 1 hour split into 900 response buckets of 4 seconds, over a 1 second collection
        assertEquals(4_000, alignmentResolution(1_000, 4_000, 3_600_000, 500));
    }

    /**
     * The response resolution is already as fine as the stored data: there is no finer grid to align on.
     */
    @Test
    public void noAlignmentWhenTheResponseMatchesTheSourceResolutionTest() {
        assertEquals(1_000, alignmentResolution(1_000, 1_000, 3_600_000, 500));
    }

    /**
     * The alignment resolution must divide the response resolution, so that an alignment interval never spans two
     * response buckets. When no divisor fits the budget, the grid degrades to the response resolution rather than
     * returning a finer but straddling one.
     */
    @Test
    public void alignmentResolutionAlwaysDividesTheResponseResolutionTest() {
        // 7 source buckets per response bucket, a budget of 10 intervals per response bucket: the source resolution fits
        assertEquals(1_000, alignmentResolution(1_000, 7_000, 70_000, 100));
        // The same with a budget of 3 intervals per response bucket: 7 being prime, no divisor between 3 and 7 exists,
        // so the only grid which both fits the budget and divides the response resolution is the response resolution
        assertEquals(7_000, alignmentResolution(1_000, 7_000, 70_000, 30));
    }

    /**
     * A merging aggregation on either axis keeps the historical behavior, whatever the budget: no alignment applies.
     * Both axes being scalar, the very same query would be aligned on a 10 seconds grid.
     */
    @Test
    public void noAlignmentWithMergingAggregationsTest() {
        assertEquals(10_000, alignmentResolution(Aggregation.AVG, Aggregation.SUM, 1_000, 60_000, 3_600_000, 500));

        assertEquals(60_000, alignmentResolution(Aggregation.MERGE, Aggregation.SUM, 1_000, 60_000, 3_600_000, 500));
        assertEquals(60_000, alignmentResolution(Aggregation.AVG, Aggregation.MERGE, 1_000, 60_000, 3_600_000, 500));
        assertEquals(60_000, alignmentResolution(Aggregation.MERGE, Aggregation.MERGE, 1_000, 60_000, 3_600_000, 500));
    }

    /**
     * An empty range shrinks into a response resolution of 0. The alignment must not divide by it.
     */
    @Test
    public void alignmentResolutionOfAnEmptyRangeTest() {
        assertEquals(0, alignmentResolution(1_000, 0, 0, 500));
    }

    /**
     * The grid is searched among the alignment intervals a response bucket may be split into, which is bounded by the
     * configured budget. The search must therefore not depend on the queried range: a response bucket covering a
     * billion source buckets and whose count of source buckets has no divisor within the budget must be resolved as
     * fast as any other one.
     */
    @Test(timeout = 5_000)
    public void alignmentResolutionIsResolvedInBoundedTimeTest() {
        // A prime number of source buckets per response bucket: no grid but the response resolution itself divides it
        assertEquals(1_000_000_007L, alignmentResolution(1, 1_000_000_007L, 1_000_000_007L, 500));
        // A billion source buckets per response bucket, split into the 500 intervals the budget affords
        assertEquals(2_000_000L, alignmentResolution(1, 1_000_000_000L, 1_000_000_000L, 500));
    }

    /**
     * Whatever the query, the alignment grid must be usable by the pipeline and must respect its budget:
     * <ul>
     *     <li>it is not finer than the stored data and not coarser than a response bucket</li>
     *     <li>it divides the response resolution, so that an interval never spans two response buckets</li>
     *     <li>it doesn't split the range into more intervals than the budget allows, unless the response buckets
     *     alone already exceed it, in which case no alignment applies</li>
     * </ul>
     */
    @Test
    public void alignmentResolutionInvariantsTest() {
        for (long sourceResolution : List.of(1_000L, 5_000L, 30_000L)) {
            // The response resolution is always a multiple of the source resolution
            for (long intervalsPerBucket : List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 10L, 12L, 60L, 120L)) {
                long resultResolution = sourceResolution * intervalsPerBucket;
                for (long resultBuckets : List.of(1L, 10L, 100L, 360L)) {
                    long rangeDiff = resultResolution * resultBuckets;
                    for (long maxAlignmentIntervals : List.of(1L, 10L, 100L, 500L, 4000L)) {
                        long alignmentResolution = alignmentResolution(sourceResolution, resultResolution, rangeDiff, maxAlignmentIntervals);
                        String message = String.format("source %d, response %d, range %d, budget %d",
                            sourceResolution, resultResolution, rangeDiff, maxAlignmentIntervals);

                        Assert.assertTrue(message, alignmentResolution >= sourceResolution);
                        Assert.assertTrue(message, alignmentResolution <= resultResolution);
                        assertEquals(message, 0, resultResolution % alignmentResolution);
                        Assert.assertTrue(message, rangeDiff / alignmentResolution <= Math.max(maxAlignmentIntervals, resultBuckets));
                    }
                }
            }
        }
    }

    /**
     * When both axes are scalar, both stages are applied on the alignment grid and their values are then reduced over
     * time. Reducing each series over the whole response bucket first would let t1/FAILED, which only existed during
     * the first source bucket, contribute its value as if it had existed during the whole range: the group-by SUM
     * would then be 20 + 20 = 40 instead of the average of the totals of the 4 source buckets, (30+30+20+20)/4 = 25.
     */
    @Test
    public void scalarAggregationsAreAppliedOnTheAlignmentGridTest() {
        TimeSeries timeSeries = newTimeSeriesWithSeriesOfDifferentLifetimes(TimeSeriesAggregationConfig.DEFAULT_MAX_ALIGNMENT_INTERVALS);

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline()
            .collect(avgOverSumQuery().split(1).build());

        assertEquals(2, response.getSeries().size());
        // The alignment grid is the source resolution, the response holds one single bucket
        assertEquals(RESOLUTION, response.getAlignmentResolution());
        assertEquals(10 * RESOLUTION, response.getResolution());

        Map<Long, Bucket> t1 = response.getSeries().get(new BucketAttributes(Map.of("name", "t1")));
        assertEquals(1, t1.size());
        Bucket t1Bucket = t1.get(0L);
        // A scalar aggregate holds one single sample, the scalar itself
        assertEquals(1, t1Bucket.getCount());
        assertEquals(25, t1Bucket.getSum());
        assertEquals(25, t1Bucket.getAverage());
        assertEquals(25, t1Bucket.getMin());
        assertEquals(25, t1Bucket.getMax());
        // The group attributes are kept and the attributes of both series are collected, whatever their lifetime
        assertEquals("t1", t1Bucket.getAttributes().get("name"));
        assertEquals(Set.of("PASSED", "FAILED"), t1Bucket.getAttributes().get("status"));

        Map<Long, Bucket> t2 = response.getSeries().get(new BucketAttributes(Map.of("name", "t2")));
        assertEquals(1, t2.size());
        assertEquals(100, t2.get(0L).getSum());
        assertEquals("t2", t2.get(0L).getAttributes().get("name"));
    }

    /**
     * The level of a scalar aggregation must not depend on the requested resolution: zooming out must not raise the
     * values. Without an alignment grid, the response bucket [0,2000) would report 20 + 20 = 40, i.e. more than any
     * of the source buckets it covers.
     */
    @Test
    public void scalarAggregationsAreStableAcrossResolutionsTest() {
        // One response bucket per source bucket: the alignment grid is the response resolution, no roll-up applies
        TimeSeriesAggregationResponse perSourceBucket = newTimeSeriesWithSeriesOfDifferentLifetimes(TimeSeriesAggregationConfig.DEFAULT_MAX_ALIGNMENT_INTERVALS)
            .getAggregationPipeline().collect(avgOverSumQuery().window(RESOLUTION).build());
        assertEquals(RESOLUTION, perSourceBucket.getAlignmentResolution());
        assertEquals(30, valueOf(perSourceBucket, "t1", 0));
        assertEquals(30, valueOf(perSourceBucket, "t1", RESOLUTION));
        assertEquals(20, valueOf(perSourceBucket, "t1", 2 * RESOLUTION));
        assertEquals(20, valueOf(perSourceBucket, "t1", 3 * RESOLUTION));

        // Two source buckets per response bucket: the totals of the source buckets are averaged over time
        TimeSeriesAggregationResponse zoomedOut = newTimeSeriesWithSeriesOfDifferentLifetimes(TimeSeriesAggregationConfig.DEFAULT_MAX_ALIGNMENT_INTERVALS)
            .getAggregationPipeline().collect(avgOverSumQuery().window(2 * RESOLUTION).build());
        assertEquals(RESOLUTION, zoomedOut.getAlignmentResolution());
        assertEquals(2 * RESOLUTION, zoomedOut.getResolution());
        // (30 + 30) / 2 and (20 + 20) / 2
        assertEquals(30, valueOf(zoomedOut, "t1", 0));
        assertEquals(20, valueOf(zoomedOut, "t1", 2 * RESOLUTION));
    }

    /**
     * A group-by SUM must not depend on the lifetime of the series it aggregates: series which never coexisted must
     * not be summed. This is the degenerate case of {@link #scalarAggregationsAreAppliedOnTheAlignmentGridTest()},
     * where every source bucket holds a different short-lived series.
     */
    @Test
    public void groupBySumIsNotInflatedBySeriesLifetimeTest() {
        int sourceBuckets = 10;
        TimeSeries timeSeries = newTimeSeries(RESOLUTION, TimeSeriesAggregationConfig.DEFAULT_MAX_ALIGNMENT_INTERVALS);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            for (int i = 0; i < sourceBuckets; i++) {
                // One distinct series per source bucket, all of them holding the same value
                ingestionPipeline.ingestPoint(Map.of("name", "t1", "id", "series-" + i), i * RESOLUTION + 1, 10L);
            }
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(
            new TimeSeriesAggregationQueryBuilder()
                .range(0, sourceBuckets * RESOLUTION)
                .split(1)
                .withTimeAggregation(Aggregation.AVG)
                .groupBy(Set.of("name"), Aggregation.SUM)
                .build());

        // At any point in time the group holds one single series worth 10. Summing the averages of the 10 series
        // instead would yield 100
        assertEquals(10, valueOf(response, "t1", 0));
    }

    /**
     * A merging time aggregation doesn't reduce the series to a scalar, so the group aggregation receives the raw
     * samples whatever the width of the response bucket: no alignment grid is required, and the historical behavior
     * must be kept untouched.
     */
    @Test
    public void mergingTimeAggregationIgnoresTheAlignmentGridTest() {
        for (int maxAlignmentIntervals : List.of(1, TimeSeriesAggregationConfig.DEFAULT_MAX_ALIGNMENT_INTERVALS)) {
            TimeSeriesAggregationResponse response = newTimeSeriesWithSeriesOfDifferentLifetimes(maxAlignmentIntervals)
                .getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
                    .range(0, 10 * RESOLUTION)
                    .window(2 * RESOLUTION)
                    .withTimeAggregation(Aggregation.MERGE)
                    .groupBy(Set.of("name"), Aggregation.SUM)
                    .build());

            String message = "maxAlignmentIntervals " + maxAlignmentIntervals;
            // No alignment grid finer than the response resolution
            assertEquals(message, 2 * RESOLUTION, response.getAlignmentResolution());
            // The sum of all the raw samples of the group: 10 + 20 + 30, then 20 + 20
            assertEquals(message, 60, valueOf(response, "t1", 0));
            assertEquals(message, 40, valueOf(response, "t1", 2 * RESOLUTION));
        }
    }

    /**
     * The alignment grid drives the memory footprint of the aggregation, so the number of alignment intervals is
     * bounded. When the budget doesn't allow for a grid finer than the response resolution, the aggregation falls
     * back to aligning on the response resolution itself.
     */
    @Test
    public void alignmentGridIsBoundedTest() {
        TimeSeriesAggregationResponse response = newTimeSeriesWithSeriesOfDifferentLifetimes(1)
            .getAggregationPipeline().collect(avgOverSumQuery().split(1).build());

        assertEquals(10 * RESOLUTION, response.getAlignmentResolution());
        assertEquals(response.getResolution(), response.getAlignmentResolution());
        // Both series are reduced over the whole range before being summed: 20 + 20
        assertEquals(40, valueOf(response, "t1", 0));
    }

    /**
     * The alignment resolution must always divide the response resolution, so that an alignment interval never spans
     * two response buckets.
     */
    @Test
    public void alignmentResolutionDividesTheResponseResolutionTest() {
        for (int maxAlignmentIntervals : List.of(1, 2, 3, 5, 7, 11, 100, 500)) {
            for (long window : List.of(RESOLUTION, 2 * RESOLUTION, 5 * RESOLUTION)) {
                TimeSeriesAggregationResponse response = newTimeSeriesWithSeriesOfDifferentLifetimes(maxAlignmentIntervals)
                    .getAggregationPipeline().collect(avgOverSumQuery().window(window).build());

                String message = "maxAlignmentIntervals " + maxAlignmentIntervals + ", window " + window;
                assertEquals(message, 0, response.getResolution() % response.getAlignmentResolution());
                Assert.assertTrue(message, response.getAlignmentResolution() >= RESOLUTION);
                Assert.assertTrue(message, response.getAlignmentResolution() <= response.getResolution());
            }
        }
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
        assertEquals(47, bucket.getAverage());
    }

    /**
     * AVG over the time window reduces the merged source buckets to the average of their raw samples.
     */
    @Test
    public void timeAggregationAvgTest() {
        // 140 / 3 => we want the rounded value which is 47 in this case
        assertEquals(47, timeAggregate(Aggregation.AVG));
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
        assertEquals(73, bucket.getAverage());
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

package step.core.timeseries;

import org.junit.Test;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.ScalarBucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link Aggregation#SAMPLED_AVG}, the time aggregation reducing a series over the whole time window rather
 * than over the samples it happens to hold.
 * <p>
 * The data sets reproduce the sampling of the controller metrics: series sampled every 15 seconds, ingested into a
 * collection of a 5 seconds resolution, and queried over one hour. A response bucket covering the whole hour is
 * therefore expected to hold 240 samples per series.
 */
public class TimeSeriesSampledAverageAggregationTest extends TimeSeriesBaseTest {

    private static final long SOURCE_RESOLUTION = 5_000;
    private static final long SAMPLING_INTERVAL = 15_000;
    private static final long ONE_HOUR = 3_600_000;
    private static final long EXPECTED_SAMPLES_PER_HOUR = ONE_HOUR / SAMPLING_INTERVAL;

    private TimeSeries newTimeSeries() {
        return getNewTimeSeries(SOURCE_RESOLUTION);
    }

    /**
     * Ingests one series sampled at the sampling interval, starting at the given timestamp.
     */
    private void ingestSampledSeries(TimeSeriesIngestionPipeline pipeline, Map<String, Object> attributes, long from, int sampleCount, long value) {
        ingestSampledSeries(pipeline, attributes, from, sampleCount, value, SAMPLING_INTERVAL);
    }

    /**
     * Ingests one series sampled at the given interval, starting at the given timestamp.
     */
    private void ingestSampledSeries(TimeSeriesIngestionPipeline pipeline, Map<String, Object> attributes, long from, int sampleCount, long value, long samplingInterval) {
        for (int i = 0; i < sampleCount; i++) {
            pipeline.ingestPoint(attributes, from + i * samplingInterval, value);
        }
    }

    /**
     * @return a query shrinking the whole hour into one single response bucket
     */
    private TimeSeriesAggregationQueryBuilder oneHourQuery() {
        return new TimeSeriesAggregationQueryBuilder()
            .range(0, ONE_HOUR)
            .split(1)
            .withSamplingInterval(SAMPLING_INTERVAL);
    }

    private long collectSingleValue(TimeSeries timeSeries, TimeSeriesAggregationQuery query) {
        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(query);
        assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(1, series.size());
        Bucket bucket = series.values().iterator().next();
        assertEquals(ScalarBucket.class, bucket.getClass());
        return ((ScalarBucket) bucket).getValue();
    }

    /**
     * A series which only existed during half of the window contributes half of its own average: 120 samples summing
     * up to 1200 are averaged over the 240 samples the hour is expected to hold, and not over their own number.
     */
    @Test
    public void samplesAreAveragedOverTheExpectedSampleCountTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, 120, 10);
        }

        assertEquals(1200 / EXPECTED_SAMPLES_PER_HOUR, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
        // The plain average ignores the half of the window the series didn't exist in
        assertEquals(10, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
    }

    /**
     * A series existing during the whole window holds exactly the expected number of samples, in which case both
     * averages are the same.
     */
    @Test
    public void sampledAverageOfACompleteSeriesIsThePlainAverageTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, (int) EXPECTED_SAMPLES_PER_HOUR, 10);
        }

        assertEquals(10, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
    }

    /**
     * The point of the sampled average: series that only exist for a part of the time range should only account for the time they existed.
     * Four agents of 40 tokens, each connected during one quarter of the hour, amount
     * to 40 tokens over the hour and not to 160.
     */
    @Test
    public void groupBySumIsNotInflatedByTheLifetimeOfTheSeriesTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            for (int agent = 0; agent < 4; agent++) {
                ingestSampledSeries(pipeline, Map.of("name", "tokens", "agent", "agent" + agent),
                    agent * (ONE_HOUR / 4), (int) EXPECTED_SAMPLES_PER_HOUR / 4, 40);
            }
        }

        assertEquals(40, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
        // Each series is averaged over its own quarter of an hour and contributes as if it had lasted the whole hour
        assertEquals(160, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
    }

    /**
     * The value a series is reduced to is not an integer, and the group aggregation receives it as is. Rounding it
     * beforehand would discard the whole contribution of the short-lived series: hundred agents contributing 0.1 each
     * amount to 10 tokens, not to 0.
     */
    @Test
    public void contributionsBelowOneAreNotLostTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            for (int agent = 0; agent < 100; agent++) {
                // 3 samples of 8, i.e. 24 over the 240 expected ones, i.e. a contribution of 0.1
                ingestSampledSeries(pipeline, Map.of("name", "tokens", "agent", "agent" + agent),
                    agent * (ONE_HOUR / 100), 3, 8);
            }
        }

        assertEquals(10, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
    }



    @Test
    public void responseResolutionIsAlignedOnTheSamplingIntervalTest() {
        // A whole number of 30 seconds windows, so that the last one isn't a partially covered one
        long range = 360_000;
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, (int) (range / SAMPLING_INTERVAL), 10);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
            .range(0, range)
            // A multiple of the 5 seconds source resolution, but not of the 15 seconds sampling interval
            .window(35_000)
            .withSamplingInterval(SAMPLING_INTERVAL)
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build());

        assertEquals(30_000, response.getResolution());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(12, series.size());
        // Every window holds the 2 samples it expects: the series reads its value everywhere
        series.forEach((begin, bucket) -> assertEquals("Bucket " + begin, 10, ((ScalarBucket) bucket).getValue()));
    }

    /**
     * A shrunk response holds one single bucket spanning the whole range, which has to be a whole number of sampling
     * intervals for the very same reason a resolution has. The requested range is rounded to the source resolution
     * only and is therefore not one, as here where it exceeds the hour by 5 seconds.
     */
    @Test
    public void shrunkRangeIsAlignedOnTheSamplingIntervalTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            // Half of the range, i.e. half of the value of the series
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, 120, 10);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
            .range(0, ONE_HOUR + 5_000)
            .split(1)
            .withSamplingInterval(SAMPLING_INTERVAL)
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build());

        // The hour and 5 seconds are expanded to the 241 sampling intervals covering them
        assertEquals(241 * SAMPLING_INTERVAL, response.getResolution());
        assertEquals(241 * SAMPLING_INTERVAL, response.getEnd());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(1, series.size());
        assertEquals(5, ((ScalarBucket) series.values().iterator().next()).getValue());
    }

    /**
     * Only the sampled average constrains the response resolution, the other aggregations keep being resolved on the
     * source resolution alone.
     */
    @Test
    public void responseResolutionIsAlignedForTheSampledAverageOnlyTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, 10, 10);
        }

        TimeSeriesAggregationQueryBuilder query = new TimeSeriesAggregationQueryBuilder()
            .range(0, 350_000)
            .window(35_000)
            .withSamplingInterval(SAMPLING_INTERVAL)
            .groupBy(Set.of("name"), Aggregation.SUM);

        assertEquals(35_000, timeSeries.getAggregationPipeline()
            .collect(query.withTimeAggregation(Aggregation.AVG).build()).getResolution());
        assertEquals(30_000, timeSeries.getAggregationPipeline()
            .collect(query.withTimeAggregation(Aggregation.SAMPLED_AVG).build()).getResolution());
    }

    /**
     * A resolution below the sampling interval cannot be aligned on it without becoming finer than the samples
     * themselves, it is therefore raised to one single sampling interval.
     */
    @Test
    public void responseResolutionIsAtLeastOneSamplingIntervalTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, 4, 10);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
            .range(0, 60_000)
            .window(SOURCE_RESOLUTION)
            .withSamplingInterval(SAMPLING_INTERVAL)
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build());

        assertEquals(SAMPLING_INTERVAL, response.getResolution());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(4, series.size());
        series.forEach((begin, bucket) -> {
            assertEquals("Bucket " + begin, 10, ((ScalarBucket) bucket).getValue());
            assertEquals("Bucket " + begin, 1, bucket.getCount());
            assertEquals("Bucket " + begin, 10, bucket.getSum());
        });
        Set.of(0L, 15_000L, 30_000L, 45_000L).forEach(begin -> assertTrue("Bucket " + begin, series.containsKey(begin)));
    }

    /**
     * The sampling interval doesn't necessarily divide the source resolution, nor the other way around, in which case
     * the response resolution has to be a multiple of both: samples every 6 seconds ingested at a resolution of 4
     * seconds are reducible on windows of 12 seconds.
     */
    @Test
    public void responseResolutionIsAMultipleOfBothTheSourceResolutionAndTheSamplingIntervalTest() {
        long sourceResolution = 4_000;
        long samplingInterval = 6_000;
        long range = 120_000;
        TimeSeries timeSeries = getNewTimeSeries(sourceResolution);
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            ingestSampledSeries(pipeline, Map.of("name", "tokens"), 0, (int) (range / samplingInterval), 10, samplingInterval);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
            .range(0, range)
            // Neither a multiple of the sampling interval nor coarser than it
            .window(sourceResolution)
            .withSamplingInterval(samplingInterval)
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build());

        assertEquals(12_000, response.getResolution());
        Map<Long, Bucket> series = response.getFirstSeries();
        // Each window holds the 2 samples it expects
        assertEquals(range / 12_000, series.size());
        series.forEach((begin, bucket) -> assertEquals("Bucket " + begin, 10, ((ScalarBucket) bucket).getValue()));
    }

    /**
     * A long range is resolved on a collection coarser than the sampling interval, whose buckets already hold several
     * samples each. The samples are counted the same way, a source bucket contributing the samples it merged and not
     * one single value.
     */
    @Test
    public void sampledAverageOnASourceResolutionCoarserThanTheSamplingIntervalTest() {
        long sourceResolution = 60_000;
        TimeSeries timeSeries = getNewTimeSeries(sourceResolution);
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            // One series over the whole hour, one over its first half only
            ingestSampledSeries(pipeline, Map.of("name", "tokens", "agent", "agent0"), 0, (int) EXPECTED_SAMPLES_PER_HOUR, 10);
            ingestSampledSeries(pipeline, Map.of("name", "tokens", "agent", "agent1"), 0, (int) EXPECTED_SAMPLES_PER_HOUR / 2, 10);
        }

        // Each window of one minute expects the 4 samples its single source bucket merged
        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
            .range(0, ONE_HOUR)
            .window(sourceResolution)
            .withSamplingInterval(SAMPLING_INTERVAL)
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build());

        assertEquals(sourceResolution, response.getResolution());
        Map<Long, Bucket> series = response.getFirstSeries();
        assertEquals(ONE_HOUR / sourceResolution, series.size());
        series.forEach((begin, bucket) ->
            // Both series during the first half of the hour, only the first one during its second half
            assertEquals("Bucket " + begin, begin < ONE_HOUR / 2 ? 20 : 10, ((ScalarBucket) bucket).getValue()));

        // Over the whole hour the second series only contributes the half of it it existed in
        assertEquals(15, collectSingleValue(timeSeries, oneHourQuery()
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.SUM)
            .build()));
    }

    /**
     * The sampled average reduces a series over a time window and is therefore only meaningful on the time axis.
     */
    @Test
    public void sampledAverageIsRejectedAsGroupAggregationTest() {
        TimeSeries timeSeries = newTimeSeries();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            timeSeries.getAggregationPipeline().collect(oneHourQuery()
                .withTimeAggregation(Aggregation.AVG)
                .groupBy(Set.of("name"), Aggregation.SAMPLED_AVG)
                .build()));
        assertTrue(exception.getMessage(), exception.getMessage().contains("only supported as a time aggregation"));
    }

    /**
     * Without the sampling interval the expected number of samples is unknown. Silently falling back to the plain
     * average would return the very values the sampled average is meant to correct, hence the query is rejected.
     */
    @Test
    public void sampledAverageIsRejectedWithoutSamplingIntervalTest() {
        TimeSeries timeSeries = newTimeSeries();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            timeSeries.getAggregationPipeline().collect(new TimeSeriesAggregationQueryBuilder()
                .range(0, ONE_HOUR)
                .split(1)
                .withTimeAggregation(Aggregation.SAMPLED_AVG)
                .groupBy(Set.of("name"), Aggregation.SUM)
                .build()));
        assertTrue(exception.getMessage(), exception.getMessage().contains("requires the sampling interval"));
    }

    /**
     * A merging group aggregation reduces the series to one sample each, the sampled average included.
     */
    @Test
    public void sampledAverageCombinesWithAMergingGroupAggregationTest() {
        TimeSeries timeSeries = newTimeSeries();
        try (TimeSeriesIngestionPipeline pipeline = timeSeries.getIngestionPipeline()) {
            // 2 series contributing 10 and 5 over the hour
            ingestSampledSeries(pipeline, Map.of("name", "tokens", "agent", "agent0"), 0, (int) EXPECTED_SAMPLES_PER_HOUR, 10);
            ingestSampledSeries(pipeline, Map.of("name", "tokens", "agent", "agent1"), 0, (int) EXPECTED_SAMPLES_PER_HOUR / 2, 10);
        }

        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(oneHourQuery()
            .withTimeAggregation(Aggregation.SAMPLED_AVG)
            .groupBy(Set.of("name"), Aggregation.MERGE)
            .build());

        Bucket bucket = response.getFirstSeries().values().iterator().next();
        assertEquals(2, bucket.getCount());
        assertEquals(15, bucket.getSum());
        assertEquals(5, bucket.getMin());
        assertEquals(10, bucket.getMax());
    }
}

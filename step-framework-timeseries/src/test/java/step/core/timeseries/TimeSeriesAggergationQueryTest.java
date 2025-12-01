package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filters;
import step.core.ql.OQLFilterBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TimeSeriesAggergationQueryTest extends TimeSeriesBaseTest {

    public static final long DURATION_T1_PASSED = 10L;
    public static final long DURATION_T1_FAILED = 20L;
    public static final long DURATION_T2_PASSED = 30L;
    public static final long DURATION_T2_FAILED = 40L;

    @Test(expected = IllegalArgumentException.class)
    public void lowInvalidResolutionTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .window(9)
                .build();
        aggregationPipeline.collect(query);
    }

    @Test
    public void lowValidResolutionTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .window(10)
                .range(0, 10000)
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(0, response.getStart());
        Assert.assertEquals(10_000, response.getEnd());
    }


    @Test
    public void shrinkTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, DURATION_T1_PASSED);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, DURATION_T1_PASSED);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, DURATION_T1_PASSED);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, DURATION_T1_PASSED);
        }

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(1)
                .build();
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        Assert.assertEquals(0, response.getStart());
        assertTrue(response.getResolution() > System.currentTimeMillis() - 3_000);
        response.getSeries().values().forEach(map -> {
            assertTrue(map.values().size() <= 1);
        });

    }

    @Test
    public void emptyFiltersTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);
        String oql = null;
        Map<String, String> params = null;
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(1)
                .withFilter(OQLFilterBuilder.getFilter(oql))
                .build();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        // we want to make sure that the methods above are not failing
        Assert.assertEquals(0, response.getSeries().size());
    }

    @Test
    public void queryIntervalPrecisionTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        int bucketsCount = 20;
        for (int i = 0; i < bucketsCount; i++) {
            Bucket bucket = new Bucket();
            bucket.setBegin(i * 1000); // one every second
            timeSeries.getDefaultCollection().save(bucket);
        }
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        String oql = "";
        int split = 5;
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 1000 * bucketsCount / 2)
                .split(split)
                .withFilter(OQLFilterBuilder.getFilter(oql))
                .build();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        Assert.assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> seriesResponse = response.getFirstSeries();
        Assert.assertEquals(split, seriesResponse.size());
        Assert.assertEquals(0, response.getStart());
        Assert.assertEquals(10_000, response.getEnd());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noRangeWithBucketsCountTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(5)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooBigSplitTest() {
        TimeSeriesCollection collection = getCollection(200);
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollection(collection)
                .setSettings(new TimeSeriesSettings().setResponseMaxIntervals(1000))
                .build();
        long now = System.currentTimeMillis();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, now)
                .split(aggregationPipeline.getResponseMaxIntervals() + 1)
                .build();

        aggregationPipeline.collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallWindowTest() {
        TimeSeriesCollection collection = getCollection(200);
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollection(collection)
                .setSettings(new TimeSeriesSettings().setResponseMaxIntervals(1000))
                .build();
        long now = System.currentTimeMillis();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - aggregationPipeline.getResponseMaxIntervals() * 1001L, now)
                .window(1000)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidIntervalTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(10000, 1000)
                .window(1000)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test
    public void responseRangeWithCustomWindowTest() {
        TimeSeries timeSeries = getNewTimeSeries(1000);
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(2000, 10_000)
                .window(5000) // should end up in 2 buckets
                .build();
        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(query);
        Assert.assertEquals(2000, response.getStart());
        Assert.assertEquals(12_000, response.getEnd());
        Assert.assertEquals(5000, response.getResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(2000, 9500)
                .window(5000) // should end up in 2 buckets
                .build();
        response = timeSeries.getAggregationPipeline().collect(query);
        Assert.assertEquals(2000, response.getStart());
        Assert.assertEquals(12_000, response.getEnd());
        Assert.assertEquals(5000, response.getResolution());
    }

    @Test
    public void aggregationTestWithResolutionChanges() {
        int resolution = 1000;
        int iterations = 1000;
        TimeSeries timeSeries = getNewTimeSeries(1000);
        Random random = new Random();
        long end = System.currentTimeMillis();
        long start = end - (iterations * resolution);
        //Ingest some data
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            for (int i = 0; i < iterations; i++ ) {
                long relativeStart = start + (i * resolution);
                int startOffset = resolution / 4;
                ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), relativeStart + random.nextInt(startOffset), DURATION_T1_PASSED);
                ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), relativeStart + startOffset + random.nextInt(startOffset), DURATION_T1_FAILED);
                ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), relativeStart + 2*startOffset + random.nextInt(startOffset), DURATION_T2_PASSED);
                ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), relativeStart + 3*startOffset + random.nextInt(startOffset), DURATION_T2_FAILED);
            }
        }
        aggregationTest(timeSeries, resolution, iterations, start, end);

        //Change timeSerie resolution on the fly to make sure this doesn't induce aggregation errors
        int newResolution = 5000;
        TimeSeriesCollection collection = new TimeSeriesCollection(timeSeries.getDefaultCollection().getMainCollection(), newResolution);
        TimeSeries timeSeriesHigherResolution = new TimeSeriesBuilder().registerCollection(collection).build();
        aggregationTest(timeSeriesHigherResolution, newResolution, iterations, start, end);
    }


    public void aggregationTest(TimeSeries timeSeries, int resolution, int iterations, long start, long end) {

        //Test aggregation with a single time bucket
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(1)
                .build();
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        Assert.assertEquals(0, response.getStart());
        //query has no time range, so the effective resolution (range used by the aggregation) is from 0 to now
        assertTrue(response.getResolution() > System.currentTimeMillis() - 3_000);
        assertEquals(resolution, response.getCollectionResolution());
        Map<BucketAttributes, Map<Long, Bucket>> series = response.getSeries();
        assertEquals(1, series.size());
        assertEquals(0, series.keySet().stream().findFirst().orElseThrow().size());
        Map<Long, Bucket> longBucketMap = series.values().stream().findFirst().orElseThrow();
        assertEquals(1, longBucketMap.size());
        assertEquals(Long.valueOf(0), longBucketMap.keySet().stream().findFirst().orElseThrow());
        Bucket bucket = longBucketMap.values().stream().findFirst().orElseThrow();
        assertEquals(iterations * 4, bucket.getCount());
        assertEquals((iterations * (DURATION_T1_FAILED + DURATION_T1_PASSED + DURATION_T2_FAILED + DURATION_T2_PASSED)), bucket.getSum());
        assertEquals(DURATION_T1_PASSED, bucket.getMin());
        assertEquals(DURATION_T2_FAILED, bucket.getMax());

        //Test aggregation with filter, grouping and 100 time buckets (since the response resolution it 10 seconds, same response is expected with source 1 and 5 seconds
        testWithManyTimeBuckets(resolution, iterations, start, end, pipeline, 100);

        //Test aggregation with filter, grouping and 1000 time buckets -> response resolution is 1 second for source 1 second, but will be 5s for the 5s source resolution
        testWithManyTimeBuckets(resolution, iterations, start, end, pipeline, 1000);
    }

    private void testWithManyTimeBuckets(int sourceResolution, int iterations, long start, long end, TimeSeriesAggregationPipeline pipeline, int targetBucketCount) {
        TimeSeriesAggregationQuery query;
        TimeSeriesAggregationResponse response;
        Map<BucketAttributes, Map<Long, Bucket>> series;
        query = new TimeSeriesAggregationQueryBuilder()
                .split(targetBucketCount)
                .withFilter(Filters.equals("attributes.name", "t1"))
                .withGroupDimensions(Set.of("status"))
                .range(start, end)
                .build();
        long expectedResponseResolution = (end - start) / targetBucketCount;
        expectedResponseResolution = (expectedResponseResolution < sourceResolution) ? sourceResolution : expectedResponseResolution;
        long expectedBucketsCount = (end - start) / expectedResponseResolution;
        response = pipeline.collect(query);
        Assert.assertEquals(expectedResponseResolution, response.getResolution());
        assertEquals(sourceResolution, response.getCollectionResolution());
        series = response.getSeries();
        assertEquals(2, series.size());
        Map<Long, Bucket> failedSeries = series.get(Map.of("status", "FAILED"));
        Map<Long, Bucket> passedSeries = series.get(Map.of("status", "PASSED"));
        validateSeries(failedSeries, DURATION_T1_FAILED, iterations, expectedBucketsCount);
        validateSeries(passedSeries, DURATION_T1_PASSED, iterations, expectedBucketsCount);
    }

    private void validateSeries(Map<Long, Bucket> series, long expectedDuration, int iterations, long targetBucketCount) {
        assertNotNull(series);
        long expectedCountPerBucket = iterations / targetBucketCount;
        long expectedCountPerBucketMin = expectedCountPerBucket - 1;
        long expectedCountPerBucketMax = expectedCountPerBucket + 1;
        //depending on the start and end time we might get back more buckets than requested to cover the range
        assertTrue("Series should have max target buckets +1 " + targetBucketCount + " time buckets, but size was: " + series.size() + "Series content: " + series, series.size() <= (targetBucketCount+1));
        //For response resolution equals source resolution, i.e. 1 second with count between 0 and 2 we might have significantly less time bucket because the one with 0 are not included
        if (expectedCountPerBucket == 1) {
            assertTrue("Series does not have sufficient number of buckets, target " + targetBucketCount + ", but size was: " + series.size() + "Series content: " + series, series.size() >= targetBucketCount*0.7);
        } else {
            assertTrue("Series should have at least the target buckets count, target " + targetBucketCount + ", but size was: " + series.size() + "Series content: " + series, series.size() >= targetBucketCount);
        }
        AtomicInteger bucketIdx = new AtomicInteger(0);
        AtomicLong totalSum = new AtomicLong(0);
        AtomicLong totalCount = new AtomicLong(0);
        series.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
            Bucket bucket = e.getValue();
            int currentIdx = bucketIdx.getAndIncrement();
            //Edge bucket can have lower c counts
            if (currentIdx > 0 && currentIdx < targetBucketCount ) {
                assertTrue("Count should be between " + expectedCountPerBucketMin + " and " + expectedCountPerBucketMax + " but was " + bucket.getCount() + ". Current idx: " + currentIdx +
                        "next bucket: " + series, expectedCountPerBucketMin <= bucket.getCount() && bucket.getCount() <= expectedCountPerBucketMax);
                assertTrue((expectedCountPerBucketMin) * expectedDuration <= bucket.getSum() && bucket.getSum() <= (expectedCountPerBucketMax) * expectedDuration);
            }
            assertEquals(expectedDuration, bucket.getMin());
            assertEquals(expectedDuration, bucket.getMax());
            totalSum.addAndGet(bucket.getSum());
            totalCount.addAndGet(bucket.getCount());
        });
        assertEquals(expectedDuration*iterations, totalSum.get());
        assertEquals(iterations, totalCount.get());
        System.out.println("totalSum: " + totalSum.get() + ", totalCount: " + totalCount.get() + ", #buckets: " + series.size());
    }

}

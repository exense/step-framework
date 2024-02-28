package step.core.timeseries;

import ch.exense.commons.test.categories.PerformanceTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.collections.inmemory.InMemoryCollectionFactory;
import step.core.collections.mongodb.MongoDBCollectionFactory;
import step.core.ql.OQLFilterBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.query.TimeSeriesQuery;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.*;

public class TimeSeriesTest {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesTest.class);

    @Test
    public void test() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 1);

        // Create 1M buckets
        long nBuckets = 1_000_000L;
        for (int i = 0; i < nBuckets; i++) {
            Bucket entity = new Bucket(1000L * i);
            entity.setCount(1);
            entity.setSum(5);
            entity.setMin(-i - 1);
            entity.setMax(i + 1);
            entity.setAttributes(new BucketAttributes(Map.of()));
            bucketCollection.save(entity);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        // Query the time series with resolution = bucket size
        Map<Long, Bucket> query = pipeline.newQueryBuilder().range(0, nBuckets * 1000)
                .window(1000)
                .build()
                .run()
                .getSeries()
                .get(new BucketAttributes(Map.of()));
        assertEquals(nBuckets, query.size());
        // Get the 2nd bucket
        Bucket actual = query.get(1000L);
        assertEquals(1000, actual.getBegin());
        assertEquals(1, actual.getCount());

        // Query the time series with resolution = bucket size on a smaller time frame
        query = pipeline.newQueryBuilder().range(1000, (nBuckets - 2) * 1000).window(1000).build().run().getSeries().get(new BucketAttributes(Map.of()));
        assertEquals(nBuckets - 3, query.size()); // -2 in the end an -1 in the beginning

        // Query the time series with resolution = size of the whole time frame
        query = pipeline.newQueryBuilder().range(0, nBuckets * 1000).window(nBuckets * 1000).build().run().getSeries().get(new BucketAttributes(Map.of()));
        assertEquals(1, query.size());
        Bucket bucket = query.get(0L);
        assertEquals(nBuckets, bucket.getCount());
        assertEquals(nBuckets * 5, bucket.getSum());
        assertEquals(-nBuckets, bucket.getMin());
        assertEquals(nBuckets, bucket.getMax());

        // Query the time series with resolution = 2 x bucket size
        query = pipeline.newQueryBuilder().range(0, nBuckets * 1000).window(2000).build().run().getSeries().get(new BucketAttributes(Map.of()));
        assertEquals(nBuckets / 2, query.size());
        assertEquals(2, query.get(0L).getCount());

    }

    @Test
    public void ingestionPipeline() {
        // Create ingestion pipeline
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 100);
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            // Ingest 1M points in series 1
            int nPoints = 1000000;
            Map<String, Object> attributes = Map.of("key", "value1");
            long start = System.currentTimeMillis();
            for (int i = 0; i < nPoints; i++) {
                ingestionPipeline.ingestPoint(attributes, 1L, 10L);
            }
            long stop = System.currentTimeMillis();
            // No automatic flushing should have occurred
            assertEquals(0, ingestionPipeline.getFlushCount());
            // Flush series to persist
            ingestionPipeline.flush();
            assertEquals(1, ingestionPipeline.getFlushCount());
            logger.info("Ingested " + nPoints + " points in " + (stop - start) + "ms. TPS = " + ((nPoints * 1000.0) / (stop - start)));

            // Ingest 1 point in series 2
            Map<String, Object> attributes2 = Map.of("key", "value2");
            ingestionPipeline.ingestPoint(attributes2, 1L, 5L);
            ingestionPipeline.flush();
            assertEquals(2, ingestionPipeline.getFlushCount());

            // Query series 1
//            Map<Long, Bucket> series = pipeline.newQueryBuilder().range(0L, 10L).withFilter(attributes).window(1000).run().getFirstSeries();
//            assertEquals(nPoints, series.get(0L).getCount());

            // Query series 2
//            series = pipeline.newQueryBuilder().range(0L, 10L).withFilter(attributes2).window(1000).run().getFirstSeries();
//            assertEquals(1, series.get(0L).getCount());

            // Query both series grouped together
//            series = pipeline.newQueryBuilder().range(0L, 10L).window(1000).run().getFirstSeries();
//            assertEquals(1 + nPoints, series.get(0L).getCount());
        }
    }

    @Test
    public void ingestionPipelineParallel() throws Exception {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        ingestionPipelineParallel(bucketCollection);
    }

    private void ingestionPipelineParallel(Collection<Bucket> bucketCollection) throws InterruptedException {
        int nThreads = 5;
        long duration = 1000;

        int timeSeriesResolution = 100;
        TimeSeries timeSeries = new TimeSeries(bucketCollection, timeSeriesResolution);
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        long start;
        LongAdder count = new LongAdder();

        Map<String, Object> attributes = Map.of("key", "value1");
        Map<String, Object> attributes2 = Map.of("key", "value2");

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline(10)) {
            ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
            start = System.currentTimeMillis();
            for (int j = 0; j < nThreads; j++) {
                executorService.submit(() -> {
                    long currentTime;
                    while ((currentTime = System.currentTimeMillis()) < start + duration) {
                        count.increment();
                        ingestionPipeline.ingestPoint(attributes, currentTime, 10L);
                        ingestionPipeline.ingestPoint(attributes2, currentTime, 5L);
                    }
                });
            }

            executorService.shutdown();
            //noinspection ResultOfMethodCallIgnored
            executorService.awaitTermination(duration * 2, TimeUnit.MILLISECONDS);
        }

        long now = System.currentTimeMillis();
        // With a resolution equal to (now-start) the query can return 1 or 2 buckets
        Map<Long, Bucket> series1 = aggregationPipeline.newQueryBuilder().withFilter(TimeSeriesFilterBuilder.buildFilter(attributes)).range(start, now).window(now - start).build().run().getFirstSeries();
        assertEquals(count.longValue(), countPoints(series1));
        assertTrue(series1.size() <= 2);

        // Split in 1 point
        series1 = aggregationPipeline
                .newQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter(attributes))
                .range(start, now)
                .split(1)
                .build()
                .run()
                .getFirstSeries();
        assertEquals(count.longValue(), countPoints(series1));
        assertEquals(1, series1.size());
        Bucket firstBucket = series1.values().stream().findFirst().get();
        assertEquals(now - now % timeSeriesResolution + timeSeriesResolution, (long) firstBucket.getEnd());
        assertEquals(start - start % timeSeriesResolution, firstBucket.getBegin());

        // Split in 2 points
        TimeSeriesAggregationResponse response = aggregationPipeline
                .newQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter(attributes))
                .range(start, now)
                .split(2)
                .build()
                .run();
        series1 = response.getFirstSeries();
        assertEquals(count.longValue(), countPoints(series1));
        assertTrue(series1.size() <=3);
        firstBucket = series1.values().stream().findFirst().get();
        assertEquals(response.getResolution(), firstBucket.getEnd() - firstBucket.getBegin());

        // Use source resolution
        series1 = aggregationPipeline
                .newQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter(attributes))
                .range(start, now).build()
                .run()
                .getFirstSeries();
        assertEquals(count.longValue(), countPoints(series1));
        assertTrue(series1.size() > duration / timeSeriesResolution);

        // Use double source resolution
        int window = timeSeriesResolution * 2;
        series1 = aggregationPipeline
                .newQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter(attributes))
                .range(start, now)
                .window(window)
                .build()
                .run()
                .getFirstSeries();
        assertEquals(count.longValue(), countPoints(series1));
        assertTrue(series1.size() > duration / window);

        // Use
        Map<Long, Bucket> series2 = aggregationPipeline
                .newQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter(attributes2))
                .range(start, now)
                .window(now - start)
                .build()
                .run()
                .getFirstSeries();
        assertEquals(count.longValue(), countPoints(series2));
        assertTrue(series2.size() <= 2);
    }

    private long countPoints(Map<Long, Bucket> series1) {
        return series1.values().stream().map(Bucket::getCount).reduce(0L, Long::sum);
    }

    @Test
    @Category(PerformanceTest.class)
    public void ingestionPipelineMongoDB() throws Exception {
        Properties properties = new Properties();
        properties.put("host", "central-mongodb.stepcloud-test.ch");
        properties.put("database", "test");
        properties.put("username", "tester");
        properties.put("password", "5dB(rs+4YRJe");
        Collection<Bucket> bucketCollection = new MongoDBCollectionFactory(properties).getCollection("series", Bucket.class);
        bucketCollection.drop();
        ingestionPipelineParallel(bucketCollection);
    }

    @Test
    public void ingestionPipelineResolution() {
        testResolution(1, 10);
        testResolution(10, 1);
        testResolution(100, 1);
    }

    private void testResolution(int resolutionMs, int expectedBucketCount) {
        // Create ingestion pipeline
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, resolutionMs);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline(resolutionMs)) {
            for (int i = 0; i < 10; i++) {
                ingestionPipeline.ingestPoint(Map.of(), i, 0L);
            }
        }
        // Assert the number of buckets persisted in the collection
        assertEquals(expectedBucketCount, bucketCollection.estimatedCount());
        // Assert the number of buckets after querying the time series
        Map<Long, Bucket> firstSeries = timeSeries.getAggregationPipeline().newQueryBuilder().build().run().getFirstSeries();
        assertEquals(expectedBucketCount, firstSeries.size());
    }

    @Test
    public void queryWithNumberOfPoints() {
        testQueryWithNumberOfPoints(2);
        testQueryWithNumberOfPoints(10);
    }

    private void testQueryWithNumberOfPoints(int numberOfBuckets) {
        // Create ingestion pipeline
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 1);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            for (int i = 0; i < 10; i++) {
                ingestionPipeline.ingestPoint(Map.of(), i, 0L);
            }
        }
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.newQueryBuilder().range(0, 10).split(numberOfBuckets).build().run();
        Map<Long, Bucket> result = response.getFirstSeries();
        assertEquals(numberOfBuckets, result.size());
        assertEquals(10, countPoints(result));

    }

    @Test
    public void testAxis() {
        // Create ingestion pipeline
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 1);
        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            for (int i = 0; i < 10; i++) {
                ingestionPipeline.ingestPoint(Map.of(), i, 0L);
            }
        }
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response;

        response = pipeline.newQueryBuilder().range(0, 10).window(5).build().run();
        assertEquals(Arrays.asList(0L, 5L), response.getAxis());

        response = pipeline.newQueryBuilder().range(0, 9).window(5).build().run();
        assertEquals(Arrays.asList(0L, 5L), response.getAxis());

        response = pipeline.newQueryBuilder().range(1, 9).window(5).build().run();
        assertEquals(Arrays.asList(1L, 6L), response.getAxis());

        response = pipeline.newQueryBuilder().range(0, 10).split(1).build().run();
        assertEquals(Arrays.asList(0L), response.getAxis());

        response = pipeline.newQueryBuilder().range(0, 9).split(2).build().run();
        assertEquals(Arrays.asList(0L, 5L), response.getAxis());

        response = pipeline.newQueryBuilder().range(1, 3).split(2).build().run();
        assertEquals(Arrays.asList(1L, 2L), response.getAxis());

        response = pipeline.newQueryBuilder().range(0, 5).window(1).build().run();
        assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), response.getAxis());

        response = pipeline.newQueryBuilder().range(0, 5).window(2).build().run();
        assertEquals(Arrays.asList(0L, 2L, 4L), response.getAxis());

        response = pipeline.newQueryBuilder().range(0, 5).split(2).build().run();
        assertEquals(Arrays.asList(0L, 3L), response.getAxis());

        response = pipeline.newQueryBuilder().range(1, 5).split(2).build().run();
        assertEquals(Arrays.asList(1L, 3L), response.getAxis());
        Map<Long, Bucket> firstSeries = response.getFirstSeries();
        assertEquals(2, firstSeries.get(1L).getCount());
        assertEquals(2, firstSeries.get(3L).getCount());
    }

    @Test
    public void testWithGroupDimensions() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 10);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        // Group
        Map<BucketAttributes, Map<Long, Bucket>> result = pipeline.newQueryBuilder().range(0, 10).withGroupDimensions(Set.of()).window(10).build().run().getSeries();
        assertEquals(4, countPoints(result.get(Map.of())));

        // Group by name
        result = pipeline.newQueryBuilder().range(0, 10).withGroupDimensions(Set.of("name")).window(10).build().run().getSeries();
        assertEquals(2, countPoints(result.get(Map.of("name", "transaction1"))));
        assertEquals(2, countPoints(result.get(Map.of("name", "transaction2"))));

        // Filter status = PASSED and group by name
        result = pipeline.newQueryBuilder().range(0, 10).withFilter(TimeSeriesFilterBuilder.buildFilter(Map.of("status", "PASSED"))).withGroupDimensions(Set.of("name")).window(10).build().run().getSeries();
        assertEquals(1, countPoints(result.get(Map.of("name", "transaction1"))));
        assertEquals(1, countPoints(result.get(Map.of("name", "transaction2"))));

        // Filter status = PASSED and group by name
        result = pipeline.newQueryBuilder().range(0, 2).withFilter(TimeSeriesFilterBuilder.buildFilter(Map.of("status", "FAILED"))).withGroupDimensions(Set.of("name", "status")).window(10).build().run().getSeries();
        assertEquals(1, countPoints(result.get(Map.of("name", "transaction1", "status", "FAILED"))));
        assertEquals(1, countPoints(result.get(Map.of("name", "transaction2", "status", "FAILED"))));
        assertNull(result.get(Map.of("name", "transaction1", "status", "PASSED")));
        assertNull(result.get(Map.of("name", "transaction2", "status", "PASSED")));
    }

    @Test
    public void testConfiguration() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();

        // No parameter
        Map<BucketAttributes, Map<Long, Bucket>> result = pipeline.newQueryBuilder().build().run().getSeries();
        // Expecting 0 dimension grouping
        assertEquals(4, countPoints(result.get(Map.of())));

        Map<Long, Bucket> series = pipeline.newQueryBuilder().build().run().getFirstSeries();
        assertEquals(4, countPoints(series));

        // Range only
        result = pipeline.newQueryBuilder().range(10, 10).build().run().getSeries();
        assertEquals(0, result.size());

        // Filter only
        result = pipeline.newQueryBuilder().build().run().getSeries();
        assertEquals(1, result.size());

        // Group
        result = pipeline.newQueryBuilder().withGroupDimensions(Set.of()).build().run().getSeries();
        assertEquals(1, result.size());

        // Group by
        result = pipeline.newQueryBuilder().withGroupDimensions(Set.of()).build().run().getSeries();
        assertEquals(1, result.size());

        // Window
        result = pipeline.newQueryBuilder().withGroupDimensions(Set.of("name")).range(0, 2).window(10).build().run().getSeries();
        assertEquals(1, result.get(Map.of("name", "transaction1")).size());

        // Split
        assertThrows(IllegalArgumentException.class, () -> pipeline.newQueryBuilder().split(2).build().run());
        result = pipeline.newQueryBuilder().withGroupDimensions(Set.of("name")).range(0, 2).split(1).build().run().getSeries();
        assertEquals(1, result.get(Map.of("name", "transaction1")).size());
    }

    @Test
    public void testHousekeeping() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "FAILED"), 2L, 10L);
        }

        Assert.assertEquals(4, bucketCollection.count(Filters.empty(), 10));

        TimeSeriesQuery timeSeriesQuery = new TimeSeriesQueryBuilder()
                .range(0L, 2L)
                .withFilter(TimeSeriesFilterBuilder.buildFilter(Map.of("name","transaction1")))
                .build();
        timeSeries.performHousekeeping(timeSeriesQuery);

        Assert.assertEquals(3, bucketCollection.count(Filters.empty(), 10));
    }

    @Test
    public void oqlTestWithoutFilter() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.newQueryBuilder()
                .range(0, 3)
                .withGroupDimensions(Set.of("status"))
                .withFilter(OQLFilterBuilder.getFilter("attributes.name = t1"))
                .build()
                .run();
        assertEquals(2, response.getSeries().size());
    }

    @Test
    public void oqlTestWithFilter() {
        InMemoryCollectionFactory collectionFactory = new InMemoryCollectionFactory(new Properties());
        TimeSeries timeSeries = new TimeSeries(collectionFactory, "buckets", 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.newQueryBuilder()
                .range(0, 3)
                .withGroupDimensions(Set.of("status"))
                .withFilter(OQLFilterBuilder.getFilter("attributes.name = t1 and attributes.status = FAILED"))
                .build()
                .run();
        assertEquals(1, response.getSeries().size());
    }

    @Test
    public void collectionFactoryConstructorTest() {
        Properties properties = new Properties();
        InMemoryCollectionFactory inMemoryCollectionFactory = new InMemoryCollectionFactory(properties);
        TimeSeries timeSeries = new TimeSeries(inMemoryCollectionFactory, "buckets", 1000);
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
    }

}

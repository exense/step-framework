package step.core.timeseries.test;

import org.junit.Test;
import step.core.collections.Collection;
import step.core.collections.inmemory.InMemoryCollectionFactory;
import step.core.timeseries.*;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TimeseriesPipelineTest {

    private static final int RESOLUTION = 1000;

    @Test
    public void test() {
        InMemoryCollectionFactory factory = new InMemoryCollectionFactory(null);
        TimeSeries timeseries = new TimeSeries(factory, "buckets", Set.of());
        TimeSeriesIngestionPipeline pipeline = timeseries.newIngestionPipeline(RESOLUTION, 3000);
        Collection<Bucket> bucketCollection = factory.getCollection("buckets", Bucket.class);
        TimeSeriesAggregationPipeline bucketService = timeseries.getAggregationPipeline(RESOLUTION);
        long nBuckets = 10;
        for (int i = 0; i < nBuckets; i++) {
            Bucket entity = new Bucket(1000L * i);
            entity.setCount(1);
            entity.setSum(5);
            entity.setMin(-i - 1);
            entity.setMax(i + 1);
            entity.setAttributes(new BucketAttributes());
            bucketCollection.save(entity);
        }

        TimeSeriesAggregationResponse response = bucketService.newQuery().range(0, nBuckets * 1000 - 1).window(1000).run();
        assertEquals(nBuckets, response.getMatrix().get(0).length);

//        // Query the time series with resolution = bucket size
//        Map<Long, Bucket> query = pipeline.query().range(0, nBuckets * 1000).window(1000).runOne();
//        assertEquals(nBuckets, query.size());
//        // Get the 2nd bucket
//        Bucket actual = query.get(1000L);
//        assertEquals(1000, actual.getBegin());
//        assertEquals(1, actual.getCount());
//
//        // Query the time series with resolution = bucket size on a smaller time frame
//        query = pipeline.query().range(1000, (nBuckets - 2) * 1000).window(1000).runOne();
//        assertEquals(nBuckets - 2, query.size());
//
//        // Query the time series with resolution = size of the whole time frame
//        query = pipeline.query().range(0, nBuckets * 1000).window(nBuckets * 1000).runOne();
//        assertEquals(1, query.size());
//        Bucket bucket = query.get(0L);
//        assertEquals(nBuckets, bucket.getCount());
//        assertEquals(nBuckets * 5, bucket.getSum());
//        assertEquals(-nBuckets, bucket.getMin());
//        assertEquals(nBuckets, bucket.getMax());
//
//        // Query the time series with resolution = 2 x bucket size
//        query = pipeline.query().range(0, nBuckets * 1000).window(2000).runOne();
//        assertEquals(nBuckets / 2, query.size());
//        assertEquals(2, query.get(0L).getCount());
    }
//
//    @Test
//    public void ingestionPipeline() {
//        // Create ingestion pipeline
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
//        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(100)) {
//            // Ingest 1M points in series 1
//            int nPoints = 1000000;
//            Map<String, Object> attributes = Map.of("key", "value1");
//            long start = System.currentTimeMillis();
//            for (int i = 0; i < nPoints; i++) {
//                ingestionPipeline.ingestPoint(attributes, 1L, 10L);
//            }
//            long stop = System.currentTimeMillis();
//            // No automatic flushing should have occurred
//            assertEquals(0, ingestionPipeline.getFlushCount());
//            // Flush series to persist
//            ingestionPipeline.flush();
//            assertEquals(1, ingestionPipeline.getFlushCount());
//            logger.info("Ingested " + nPoints + " points in " + (stop - start) + "ms. TPS = " + ((nPoints * 1000.0) / (stop - start)));
//
//            // Ingest 1 point in series 2
//            Map<String, Object> attributes2 = Map.of("key", "value2");
//            ingestionPipeline.ingestPoint(attributes2, 1L, 5L);
//            ingestionPipeline.flush();
//            assertEquals(2, ingestionPipeline.getFlushCount());
//
//            // Query series 1
//            Map<Long, Bucket> series = pipeline.query().range(0L, 10L).filter(attributes).window(1000).runOne();
//            assertEquals(nPoints, series.get(0L).getCount());
//
//            // Query series 2
//            series = pipeline.query().range(0L, 10L).filter(attributes2).window(1000).runOne();
//            assertEquals(1, series.get(0L).getCount());
//
//            // Query both series grouped together
//            series = pipeline.query().range(0L, 10L).window(1000).runOne();
//            assertEquals(1 + nPoints, series.get(0L).getCount());
//        }
//    }
//
//    @Test
//    public void ingestionPipelineParallel() throws Exception {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        ingestionPipelineParallel(bucketCollection);
//    }
//
//    private void ingestionPipelineParallel(Collection<Bucket> bucketCollection) throws InterruptedException {
//        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
//
//        long start = System.currentTimeMillis();
//        long duration = 1000;
//        LongAdder count = new LongAdder();
//
//        Map<String, Object> attributes = Map.of("key", "value1");
//        Map<String, Object> attributes2 = Map.of("key", "value2");
//
//        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newAutoFlushingIngestionPipeline(100, 10)) {
//            ExecutorService executorService = Executors.newFixedThreadPool(5);
//            for (int j = 0; j < 5; j++) {
//                executorService.submit(() -> {
//                    long currentTime;
//                    while ((currentTime = System.currentTimeMillis()) < start + duration) {
//                        count.increment();
//                        ingestionPipeline.ingestPoint(attributes, currentTime, 10L);
//                        ingestionPipeline.ingestPoint(attributes2, currentTime, 5L);
//                    }
//                });
//            }
//
//            executorService.shutdown();
//            //noinspection ResultOfMethodCallIgnored
//            executorService.awaitTermination(duration * 2, TimeUnit.MILLISECONDS);
//        }
//
//        long now = System.currentTimeMillis();
//        // With a resolution equal to (now-start) the query can return 1 or 2 buckets
//        Map<Long, Bucket> series1 = pipeline.query().filter(attributes).range(start - 1000L, now).window(now - start).runOne();
//        assertEquals(count.longValue(), countPoints(series1));
//        assertTrue(series1.size() <= 2);
//
//        series1 = pipeline.query().filter(attributes).range(start - 1000L, now).window(1).runOne();
//        assertEquals(count.longValue(), countPoints(series1));
//        assertTrue(series1.size() <= 11);
//
//        Map<Long, Bucket> series2 = pipeline.query().filter(attributes2).range(start - 1000L, now).window(now - start).runOne();
//        assertEquals(count.longValue(), countPoints(series2));
//        assertTrue(series2.size() <= 2);
//    }
//
//    private long countPoints(Map<Long, Bucket> series1) {
//        return series1.values().stream().map(Bucket::getCount).reduce(0L, Long::sum);
//    }
//
//    @Test
//    public void ingestionPipelineMongoDB() throws Exception {
//        Properties properties = new Properties();
//        properties.put("host", "localhost");
//        properties.put("database", "time-series-test");
//        Collection<Bucket> bucketCollection = new MongoDBCollectionFactory(properties).getCollection("series", Bucket.class);
//        bucketCollection.drop();
//        ingestionPipelineParallel(bucketCollection);
//    }
//
//    @Test
//    public void ingestionPipelineResolution() {
//        testResolution(1, 10);
//        testResolution(10, 1);
//        testResolution(100, 1);
//    }
//
//    private void testResolution(int resolutionMs, int expectedBucketCount) {
//        // Create ingestion pipeline
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
//        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(resolutionMs)) {
//            for (int i = 0; i < 10; i++) {
//                ingestionPipeline.ingestPoint(Map.of(), i, 0L);
//            }
//        }
//        assertEquals(expectedBucketCount, bucketCollection.estimatedCount());
//    }
//
//    @Test
//    public void queryWithNumberOfPoints() {
//        testQueryWithNumberOfPoints(2, 2);
//        testQueryWithNumberOfPoints(10, 10);
//    }
//
//    private void testQueryWithNumberOfPoints(int numberOfPoints, int expectedBucketCount) {
//        // Create ingestion pipeline
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
//        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(1)) {
//            for (int i = 0; i < 10; i++) {
//                ingestionPipeline.ingestPoint(Map.of(), i, 0L);
//            }
//        }
////        Map<Long, Bucket> result = pipeline.query().range(0, 10).split(numberOfPoints).runOne();
////        assertEquals(expectedBucketCount, result.size());
////        assertEquals(10, countPoints(result));
//    }
//
//    @Test
//    public void testGroupBy() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
//
//        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(100)) {
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "PASSED"), 1L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "FAILED"), 2L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "PASSED"), 1L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "FAILED"), 2L, 10L);
//        }
//
//        // Group
//        Map<Map<String, String>, Map<Long, Bucket>> result = pipeline.query().range(0, 10).group().window(10).run();
//        assertEquals(4, countPoints(result.get(Map.of())));
//
//        // Group by name
//        result = pipeline.query().range(0, 10).groupBy(Set.of("name")).window(10).run();
//        assertEquals(2, countPoints(result.get(Map.of("name", "transaction1"))));
//        assertEquals(2, countPoints(result.get(Map.of("name", "transaction2"))));
//
//        // Filter status = PASSED and group by name
//        result = pipeline.query().range(0, 10).filter(Map.of("status", "PASSED")).groupBy(Set.of("name")).window(10).run();
//        assertEquals(1, countPoints(result.get(Map.of("name", "transaction1"))));
//        assertEquals(1, countPoints(result.get(Map.of("name", "transaction2"))));
//
//        // Filter status = PASSED and group by name
//        result = pipeline.query().range(0, 2).filter(Map.of("status", "FAILED")).groupBy(Set.of("name", "status")).window(1).run();
//        assertEquals(1, countPoints(result.get(Map.of("name", "transaction1", "status", "FAILED"))));
//        assertEquals(1, countPoints(result.get(Map.of("name", "transaction2", "status", "FAILED"))));
//        assertNull(result.get(Map.of("name", "transaction1", "status", "PASSED")));
//        assertNull(result.get(Map.of("name", "transaction2", "status", "PASSED")));
//    }
//
//    @Test
//    public void testConfiguration() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
//
//        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(1)) {
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "PASSED"), 1L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction1", "status", "FAILED"), 2L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "PASSED"), 1L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "transaction2", "status", "FAILED"), 2L, 10L);
//        }
//
//        // No parameter
//        Map<Map<String, String>, Map<Long, Bucket>> result = pipeline.query().run();
//        // Expecting 0 dimension grouping
//        assertEquals(4, countPoints(result.get(Map.of())));
//
//        Map<Long, Bucket> series = pipeline.query().runOne();
//        assertEquals(4, countPoints(series));
//
//        // Range only
//        result = pipeline.query().range(10, 10).run();
//        assertEquals(0, result.size());
//
//        // Filter only
//        result = pipeline.query().filter(Map.of()).run();
//        assertEquals(1, result.size());
//
//        // Group
//        result = pipeline.query().group().run();
//        assertEquals(1, result.size());
//
//        // Group by
//        result = pipeline.query().groupBy(Set.of()).run();
//        assertEquals(1, result.size());
//
//        // Window
//        result = pipeline.query().groupBy(Set.of("name")).window(10).run();
//        assertEquals(1, result.get(Map.of("name", "transaction1")).size());
//
//        // Split
//        assertThrows(IllegalArgumentException.class, () -> pipeline.query().split(2).run());
//        assertThrows(IllegalArgumentException.class, () -> pipeline.query().range(1, 2).split(2).run());
//        result = pipeline.query().groupBy(Set.of("name")).range(0, 2).split(2).run();
//        assertEquals(2, result.get(Map.of("name", "transaction1")).size());
//    }
}

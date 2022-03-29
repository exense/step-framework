package step.core.timeseries;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.collections.mongodb.MongoDBCollectionFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeSeriesPipelineTest {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesPipelineTest.class);

    @Test
    public void test() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);

        // Create 1M buckets
        long nBuckets = 100000L;
        for (int i = 0; i < nBuckets; i++) {
            Bucket entity = new Bucket(1000L * i);
            entity.setCount(1);
            entity.setSum(5);
            entity.setMin(-i - 1);
            entity.setMax(i + 1);
            bucketCollection.save(entity);
        }

        // Query the time series with resolution = bucket size
        Map<Long, Bucket> query = pipeline.query(Map.of(), 0, nBuckets * 1000, 1000);
        assertEquals(nBuckets, query.size());
        // Get the 2nd bucket
        Bucket actual = query.get(1000L);
        assertEquals(1000, actual.getBegin());
        assertEquals(1, actual.getCount());

        // Query the time series with resolution = bucket size on a smaller time frame
        query = pipeline.query(Map.of(), 1000, (nBuckets - 2) * 1000, 1000);
        assertEquals(nBuckets - 2, query.size());

        // Query the time series with resolution = size of the whole time frame
        query = pipeline.query(Map.of(), 0, nBuckets * 1000, nBuckets * 1000);
        assertEquals(1, query.size());
        Bucket bucket = query.get(0L);
        assertEquals(nBuckets, bucket.getCount());
        assertEquals(nBuckets * 5, bucket.getSum());
        assertEquals(-nBuckets, bucket.getMin());
        assertEquals(nBuckets, bucket.getMax());

        // Query the time series with resolution = 2 x bucket size
        query = pipeline.query(Map.of(), 0, nBuckets * 1000, 2000);
        assertEquals(nBuckets / 2, query.size());
        assertEquals(2, query.get(0L).getCount());
    }

    @Test
    public void ingestionPipeline() {
        // Create ingestion pipeline
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(100)) {
            // Ingest 1M points in series 1
            int nPoints = 1000000;
            Map<String, String> attributes = Map.of("key", "value1");
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
            Map<String, String> attributes2 = Map.of("key", "value2");
            ingestionPipeline.ingestPoint(attributes2, 1L, 5L);
            ingestionPipeline.flush();
            assertEquals(2, ingestionPipeline.getFlushCount());

            // Query series 1
            Map<Long, Bucket> series = pipeline.query(attributes, 0L, 10L, 1000);
            assertEquals(nPoints, series.get(0L).getCount());

            // Query series 2
            series = pipeline.query(attributes2, 0L, 10, 1000);
            assertEquals(1, series.get(0L).getCount());

            // Query both series grouped together
            series = pipeline.query(Map.of(), 0L, 10, 1000);
            assertEquals(1 + nPoints, series.get(0L).getCount());
        }
    }

    @Test
    public void ingestionPipelineParallel() throws Exception {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        ingestionPipelineParallel(bucketCollection);
    }

    private void ingestionPipelineParallel(Collection<Bucket> bucketCollection) throws InterruptedException {
        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);

        long start = System.currentTimeMillis();
        long duration = 1000;
        LongAdder count = new LongAdder();

        Map<String, String> attributes = Map.of("key", "value1");
        Map<String, String> attributes2 = Map.of("key", "value2");

        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newAutoFlushingIngestionPipeline(100, 10)) {
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            for (int j = 0; j < 5; j++) {
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
        Map<Long, Bucket> series1 = pipeline.query(attributes, start - 1000L, now, now - start);
        assertEquals(count.longValue(), countPoints(series1));
        assertTrue(series1.size() <= 2);

        series1 = pipeline.query(attributes, start - 1000L, now, 1);
        assertEquals(count.longValue(), countPoints(series1));
        assertTrue(series1.size() <= 11);

        Map<Long, Bucket> series2 = pipeline.query(attributes2, start - 1000L, now, now - start);
        assertEquals(count.longValue(), countPoints(series2));
        assertTrue(series2.size() <= 2);
    }

    private long countPoints(Map<Long, Bucket> series1) {
        return series1.values().stream().map(Bucket::getCount).reduce(0L, Long::sum);
    }

    @Test
    public void ingestionPipelineMongoDB() throws Exception {
        Properties properties = new Properties();
        properties.put("host", "localhost");
        properties.put("database", "time-series-test");
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
        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(bucketCollection);
        try (TimeSeriesPipeline.IngestionPipeline ingestionPipeline = pipeline.newIngestionPipeline(resolutionMs)) {
            for (int i = 0; i < 10; i++) {
                ingestionPipeline.ingestPoint(Map.of(), i, 0L);
            }
        }
        assertEquals(expectedBucketCount, bucketCollection.estimatedCount());
    }
}
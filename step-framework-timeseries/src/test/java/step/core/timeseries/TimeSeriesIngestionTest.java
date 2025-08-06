package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filters;
import step.core.timeseries.bucket.Bucket;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimeSeriesIngestionTest extends TimeSeriesBaseTest {

    @Test
    public void ingestDataForEmptyCollectionsWithNoTTL() {
        List<TimeSeriesCollection> collections = Arrays.asList(
                getCollection(100),
                getCollection(200),
                getCollection(400),
                getCollection(800),
                getCollection(1600)
        );
        try (TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build()) {
            Bucket bucket = getRandomBucket();
            collections.get(0).save(bucket);
            timeSeries.ingestDataForEmptyCollections();
        }
        collections.forEach(c -> Assert.assertEquals(1, c.count(Filters.empty(), null)));
    }

    @Test
    public void ingestDataForEmptyCollectionsWithNoTTLWithDataInMiddle() {
        List<TimeSeriesCollection> collections = Arrays.asList(
                getCollection(100),
                getCollection(200),
                getCollection(400), // this is having data
                getCollection(800),
                getCollection(1600)
        );
        try (TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build()) {
            Bucket bucket = getRandomBucket();
            int indexWithData = 2;
            collections.get(indexWithData).save(bucket);
            timeSeries.ingestDataForEmptyCollections();
            for (int i = 0; i < collections.size(); i++) {
                long expectedData = 0;
                if (i >= indexWithData) {
                    expectedData = 1;
                }
                Assert.assertEquals(expectedData, collections.get(i).count(Filters.empty(), null));
            }
        }
    }

    @Test
    public void ingestAllCollectionsWhenNoDataExist() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(100, 200, 400, 800, 1600);
        timeSeries.ingestDataForEmptyCollections();
        assertAllCollectionsAreEmpty(timeSeries);
    }

    @Test
    public void ingestionCloseTest() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(100, 200, 400, 800, 1600);
        Map<String, Object> attributes = Map.of("key", "value1");
        timeSeries.getIngestionPipeline().ingestPoint(attributes, 10, 10);
        assertAllCollectionsAreEmpty(timeSeries);
        timeSeries.close(); // should also flush
        timeSeries.getCollections().forEach(c -> Assert.assertEquals(1, c.count(Filters.empty(), null)));
    }

    @Test
    public void flushShouldNotPropagateTest() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(100, 200, 400, 800, 1600);
        Map<String, Object> attributes = Map.of("key", "value1");
        timeSeries.getIngestionPipeline().ingestPoint(attributes, 10, 10);
        timeSeries.getDefaultCollection().getIngestionPipeline().ingestPoint(attributes, 10, 10);
        assertAllCollectionsAreEmpty(timeSeries);
        timeSeries.getIngestionPipeline().flush();
        Assert.assertEquals(1, timeSeries.getCollections().get(0).count(Filters.empty(), null));
        for (int i = 1; i < timeSeries.getCollections().size(); i++) { // skip first collection
            Assert.assertEquals(0, timeSeries.getCollections().get(i).count(Filters.empty(), null));
        }
        timeSeries.getCollections().get(1).getIngestionPipeline().flush();
        Assert.assertEquals(1, timeSeries.getCollections().get(1).count(Filters.empty(), null));
        for (int i = 2; i < timeSeries.getCollections().size(); i++) { // skip first collection
            Assert.assertEquals(0, timeSeries.getCollections().get(i).count(Filters.empty(), null));
        }
        timeSeries.close();//Close ensure that everything is flushed and persisted
        for (int i = 2; i < timeSeries.getCollections().size(); i++) { // skip first collection
            Assert.assertEquals(1, timeSeries.getCollections().get(i).count(Filters.empty(), null));
        }
    }

    @Test
    public void initialIngestionWhenCollectionIsNotEmptyTest() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(100, 200, 400, 800, 1600);
        Bucket b1 = getRandomBucket();
        Bucket b2 = getRandomBucket();
        timeSeries.getCollections().get(0).save(b1);
        timeSeries.getCollections().get(1).save(b2);
        timeSeries.ingestDataForEmptyCollections();
        timeSeries.getCollections().forEach(c -> c.getIngestionPipeline().flush());
        timeSeries.getCollections().forEach(c -> Assert.assertEquals(1, c.count(Filters.empty(), null)));
        for (int i = 2; i < timeSeries.getCollections().size(); i++) {
            Bucket bucket = timeSeries.getCollections().get(i).find(Filters.empty()).findFirst().orElseThrow();
            // make sure they are the second bucket only
            Assert.assertEquals(bucket.getCount(), b2.getCount());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeTtlTest() {
        getCollectionWithTTL(1000, -100);
    }

    @Test
    public void initialIngestionWithTTLNotInRangeTest() {
        List<TimeSeriesCollection> collections = Arrays.asList(
                getCollectionWithTTL(100, 1000),
                getCollectionWithTTL(200, 1000),
                getCollectionWithTTL(400, 1000),
                getCollectionWithTTL(800, 1000)
        );
        try (TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build()) {
            Bucket b1 = getRandomBucket();
            b1.setBegin(10); // ttl should not match it
            timeSeries.getCollections().get(0).save(b1);
            timeSeries.ingestDataForEmptyCollections();
            assertCollectionsAreEmpty(collections.subList(1, timeSeries.getCollections().size()));
        }
    }

    @Test
    public void initialIngestionWithTTLInRangeTest() {
        List<TimeSeriesCollection> collections = Arrays.asList(
                getCollectionWithTTL(100, 2000),
                getCollectionWithTTL(200, 2000),
                getCollectionWithTTL(400, 2000),
                getCollectionWithTTL(800, 500)
        );
        try (TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build()) {
            Bucket b1 = getRandomBucket();
            b1.setBegin(System.currentTimeMillis() - 1000); // ttl should match it
            timeSeries.getCollections().get(0).save(b1);
            timeSeries.ingestDataForEmptyCollections();
            for (int i = 0; i < timeSeries.getCollections().size() - 1; i++) {
                Assert.assertEquals(1, collections.get(i).count(Filters.empty(), null));
            }
        }
        Assert.assertEquals(0, collections.get(collections.size() - 1).count(Filters.empty(), null));
    }

    @Test
    public void ingestionWithPartialTTLMatchingTest() {
        List<TimeSeriesCollection> collections = Arrays.asList(
                getCollectionWithTTL(1000, 1000),
                getCollectionWithTTL(2000, 1000)
        );
        try (TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build()) {
            long now = System.currentTimeMillis();
            Bucket b1 = getRandomBucket();
            Bucket b2 = getRandomBucket();
            b1.setBegin(now - 2000); // should not be ingested in the second collection
            b2.setBegin(now - 500);
            timeSeries.getDefaultCollection().save(Arrays.asList(b1, b2));
            timeSeries.ingestDataForEmptyCollections();

            List<Bucket> foundBuckets = collections.get(1).find(Filters.empty()).collect(Collectors.toList());
            Assert.assertEquals(1, foundBuckets.size());
            Bucket foundBucket = foundBuckets.get(0);
            Assert.assertEquals(b2.getCount(), foundBucket.getCount());
        }
    }

    @Test
    public void ingestionWithManyBucketsTest() {
        try (TimeSeries timeSeries = getTimeSeriesWithResolutions(1000, 5000, 30_000)) {
            long min = Long.MAX_VALUE;
            long max = 0;
            long count = 0;
            long sum = 0;
            for (int i = 0; i < 1000; i++) {
                Bucket b = getRandomBucket();
                count += b.getCount();
                min = Math.min(min, b.getMin());
                max = Math.max(max, b.getMax());
                sum += b.getSum();
                timeSeries.getDefaultCollection().save(b);
            }
            timeSeries.ingestDataForEmptyCollections();
            TimeSeriesCollection lastCollection = timeSeries.getCollections().get(2);

            List<Bucket> foundBuckets = lastCollection.find(Filters.empty()).collect(Collectors.toList());
            Assert.assertEquals(1, foundBuckets.size());
            Bucket foundBucket = foundBuckets.get(0);
            Assert.assertEquals(count, foundBucket.getCount());
            Assert.assertEquals(sum, foundBucket.getSum());
            Assert.assertEquals(max, foundBucket.getMax());
            Assert.assertEquals(min, foundBucket.getMin());
        }
    }

    @Test
    public void ingestionExceedingQueueSizeTest() throws InterruptedException {
        TimeSeriesCollectionSettings timeSeriesCollectionSettings = new TimeSeriesCollectionSettings();
        timeSeriesCollectionSettings.setIngestionFlushAsyncQueueSize(1000);
        timeSeriesCollectionSettings.setIngestionFlushingPeriodMs(0); //flush is called directly in the test
        timeSeriesCollectionSettings.setResolution(30_000);
        try (TimeSeries timeSeries = getTimeSeriesWithSettings(timeSeriesCollectionSettings)) {
            List<Bucket> collect = timeSeries.getDefaultCollection().find(Filters.empty()).collect(Collectors.toList());
            Assert.assertEquals(0, collect.size());
            for (int i = 0; i < 999; i++) {
                Bucket b = getCurrentUniqueRandomBucket();
                timeSeries.getIngestionPipeline().ingestBucket(b);
            }
            timeSeries.getIngestionPipeline().flush(false); // simulate flush async job, resolution not reached and queue size not reached
            collect = timeSeries.getDefaultCollection().find(Filters.empty()).collect(Collectors.toList());
            Assert.assertEquals(0, collect.size());
            Bucket b = getCurrentUniqueRandomBucket();
            timeSeries.getIngestionPipeline().ingestBucket(b);
            timeSeries.getIngestionPipeline().flush(false); // simulate flush async job, now we have over the queue size
            collect = timeSeries.getDefaultCollection().find(Filters.empty()).collect(Collectors.toList());
            Assert.assertEquals(1000, collect.size());
        }
    }

}

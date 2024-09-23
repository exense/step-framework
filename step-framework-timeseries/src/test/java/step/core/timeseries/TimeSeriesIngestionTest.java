package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.core.timeseries.bucket.Bucket;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build();
        Bucket bucket = getRandomBucket();
        collections.get(0).getCollection().save(bucket);
        timeSeries.ingestDataForEmptyCollections();
        collections.forEach(c -> Assert.assertEquals(1, c.getCollection().count(Filters.empty(), null)));
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
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollections(collections).build();
        Bucket bucket = getRandomBucket();
        int indexWithData = 2;
        collections.get(indexWithData).getCollection().save(bucket);
        timeSeries.ingestDataForEmptyCollections();
        for (int i = 0; i < collections.size(); i++) {
            long expectedData = 0;
            if (i >= indexWithData) {
                expectedData = 1;
            }
            Assert.assertEquals(expectedData, collections.get(i).getCollection().count(Filters.empty(), null));
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
        timeSeries.getCollections().forEach(c -> Assert.assertEquals(1, c.getCollection().count(Filters.empty(), null)));
    }

    @Test
    public void flushShouldNotPropagateTest() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(100, 200, 400, 800, 1600);
        Map<String, Object> attributes = Map.of("key", "value1");
        timeSeries.getIngestionPipeline().ingestPoint(attributes, 10, 10);
        timeSeries.getDefaultCollection().getIngestionPipeline().ingestPoint(attributes, 10, 10);
        assertAllCollectionsAreEmpty(timeSeries);
        timeSeries.getIngestionPipeline().flush();
        Assert.assertEquals(1, timeSeries.getCollections().get(0).getCollection().count(Filters.empty(), null));
        for (int i = 1; i < timeSeries.getCollections().size(); i++) { // skip first collection
            Assert.assertEquals(0, timeSeries.getCollections().get(i).getCollection().count(Filters.empty(), null));
        }
        timeSeries.getCollections().get(1).getIngestionPipeline().flush();
        Assert.assertEquals(1, timeSeries.getCollections().get(1).getCollection().count(Filters.empty(), null));
        for (int i = 2; i < timeSeries.getCollections().size(); i++) { // skip first collection
            Assert.assertEquals(0, timeSeries.getCollections().get(i).getCollection().count(Filters.empty(), null));
        }
    }

    @Test
    public void initialIngestionWhenCollectionIsNotEmptyTest() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(100, 200, 400, 800, 1600);
        Bucket b1 = getRandomBucket();
        Bucket b2 = getRandomBucket();
        timeSeries.getCollections().get(0).getCollection().save(b1);
        timeSeries.getCollections().get(1).getCollection().save(b2);
        timeSeries.ingestDataForEmptyCollections();
        timeSeries.getCollections().forEach(c -> c.getIngestionPipeline().flush());
        timeSeries.getCollections().forEach(c -> Assert.assertEquals(1, c.getCollection().count(Filters.empty(), null)));
        for (int i = 2; i < timeSeries.getCollections().size(); i++) {
            Bucket bucket = timeSeries.getCollections().get(i).getCollection().find(Filters.empty(), null, null, null, 0).findFirst().get();
            // make sure they are the second bucket only
            Assert.assertEquals(bucket.getCount(), b2.getCount());
        }
    }


}

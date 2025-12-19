package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class TimeSeriesHousekeepingTest extends TimeSeriesBaseTest {

    @Test
    public void simpleCleanupTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        int bucketsCount = 20;
        for (int i = 0; i < bucketsCount; i++) {
            Bucket bucket = new Bucket();
            bucket.setBegin(i * 1000);
            timeSeries.getDefaultCollection().save(bucket);
        }
        Assert.assertEquals(20, timeSeries.getDefaultCollection().count(Filters.empty(), null));
        timeSeries.performHousekeeping(new TimeSeriesAggregationQueryBuilder().build());
        Assert.assertEquals(0, timeSeries.getDefaultCollection().count(Filters.empty(), null));
    }

    @Test
    public void ttlNotCoveringTest() {
        TimeSeriesCollection tsCol1 = new TimeSeriesCollection(new InMemoryCollection<>(), new TimeSeriesCollectionSettings().setResolution(10).setTtl(40)); // this live longer
        TimeSeriesCollection tsCol2 = new TimeSeriesCollection(new InMemoryCollection<>(), new TimeSeriesCollectionSettings().setResolution(100).setTtl(10_000)); // this should not cover the request
        long now = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            Bucket bucket = new Bucket();
            bucket.setBegin(now - 50);
            bucket.setCount(1);
            bucket.setAttributes(new BucketAttributes(Map.of("col", "1")));
            tsCol1.save(bucket);
        }
        Bucket col2Bucket = new Bucket();
        col2Bucket.setBegin(now - 50);
        col2Bucket.setCount(20);
        col2Bucket.setAttributes(new BucketAttributes(Map.of("col", "2")));
        tsCol2.save(col2Bucket);
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollections(Arrays.asList(tsCol1, tsCol2))
                .setSettings(new TimeSeriesSettings()
                        .setTtlEnabled(true))
                .build();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 1000, now) // col1 collection should be ignored, even if it is the ideal target
                .split(1)
                .withGroupDimensions(Set.of("col"))
                .build();
        Map<BucketAttributes, Map<Long, Bucket>> response = timeSeries.getAggregationPipeline().collect(query).getSeries();
        Assert.assertEquals(1, response.size());
        BucketAttributes bucketAttributes = new ArrayList<>(response.keySet()).get(0);

        // all possible buckets belong to col2
        Assert.assertEquals("2", bucketAttributes.get("col"));
    }

    @Test
    public void ttlNotCovered() {
        TimeSeriesCollection collection = getCollectionWithTTL(1000, 50_000);
        long now = System.currentTimeMillis();
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollection(collection)
                .setSettings(new TimeSeriesSettings()
                        .setTtlEnabled(true))
                .build();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 30_000, now)
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 60_000, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());

        timeSeries.setTtlEnabled(false);
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 60_000, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
    }

}

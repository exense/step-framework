package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filters;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TimeSeriesWithAttributesTest extends TimeSeriesBaseTest {

    @Test
    public void simpleQueryWithNoAttributesUsed() {
        TimeSeriesCollection collection = getCollection(1000, Set.of("a", "b", "c"));
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
        Bucket randomBucket = getRandomBucket();
        randomBucket.setBegin(1);
        ingestionPipeline.ingestBucket(randomBucket);
        ingestionPipeline.flush();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertFalse(response.isHigherResolutionUsed());

    }

    @Test
    public void simpleCollectionWithGroupingAttributes() {
        TimeSeriesCollection collection = getCollection(1000, Set.of("a", "b", "c"));
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();
        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        Bucket randomBucket = getRandomBucket();
        randomBucket.setBegin(1);
        randomBucket.getAttributes().put("a", "value");
        randomBucket.getAttributes().put("z", "missing");
        ingestionPipeline.ingestBucket(randomBucket);
        ingestionPipeline.flush();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("a"))
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        Map<Long, Bucket> buckets = response.getSeries().get(Map.of("a", "value"));
        Assert.assertNotNull(buckets);
        Assert.assertNotNull(buckets.get(0L));
        Assert.assertTrue(response.isTtlCovered());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("z"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        buckets = response.getSeries().get(Map.of());
        Assert.assertNotNull(buckets);
        Bucket bucket = collection.getCollection().find(Filters.empty(), null, null, null, 0).collect(Collectors.toList()).get(0);
        Assert.assertNull(bucket.getAttributes().get("z"));
        Assert.assertNotNull(bucket.getAttributes().get("a"));
    }

    @Test
    public void simpleCollectionWithFilteringAndGroupingAttributes() {
        TimeSeriesCollection collection = getCollection(1000, Set.of("a", "b", "c"));
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();
        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        Bucket randomBucket = getRandomBucket();
        randomBucket.setBegin(1);
        randomBucket.getAttributes().put("a", "valueA");
        randomBucket.getAttributes().put("b", "valueB");
        randomBucket.getAttributes().put("d", "valueD"); // not handled
        randomBucket.getAttributes().put("z", "missing");
        ingestionPipeline.ingestBucket(randomBucket);
        ingestionPipeline.flush();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("a"))
                .withFilter(Map.of("b", "valueB"))
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        Map<Long, Bucket> buckets = response.getSeries().get(Map.of("a", "valueA"));
        Assert.assertNotNull(buckets);
        Assert.assertNotNull(buckets.get(0L));

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("a"))
                .withFilter(Map.of("d", "valueD"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(0, response.getSeries().size());
    }

    @Test
    public void fallBackToAnotherCollectionBecauseOfAttributes() {
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollections(List.of(
                        getCollection(1000, Set.of("a", "b", "c")),
                        getCollection(2000, Set.of("a", "b", "c")),
                        getCollection(10000, Set.of("a", "b")),
                        getCollection(20000, Set.of("a"))
                ))
                .build();
        Bucket bucket = getRandomBucket();
        bucket.setBegin(5000);
        bucket.getAttributes().put("a", "1");
        bucket.getAttributes().put("b", "2");
        bucket.getAttributes().put("c", "3");
        bucket.getAttributes().put("d", "4");
        timeSeries.getDefaultCollection().getIngestionPipeline().ingestBucket(bucket);

        timeSeries.getCollections().forEach(c -> {
            c.getIngestionPipeline().flush();
            Bucket foundBucket = c.getCollection().find(Filters.empty(), null, null, null, 0).collect(Collectors.toList()).get(0);
            Assert.assertEquals(bucket.getCount(), foundBucket.getCount());
            c.getHandledAttributes().forEach(attr -> Assert.assertTrue(foundBucket.getAttributes().containsKey(attr)));
            Assert.assertEquals(c.getHandledAttributes().size(), foundBucket.getAttributes().size());
        });

        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1) // to make sure 3rd interval is chosen
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(10_000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1) // to make sure 3rd interval is chosen
                .withGroupDimensions(Set.of("c"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(2000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1) // to make sure 3rd interval is chosen
                .withGroupDimensions(Set.of("c"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(2000, response.getCollectionResolution());
//
        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1) // to make sure 3rd interval is chosen
                .withFilter(Map.of("c", "3"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(2000, response.getCollectionResolution()); // first collection

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1) // to make sure 3rd interval is chosen
                .withFilter(Map.of("unknown", "1"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(1000, response.getCollectionResolution()); // first collection
    }

    @Test
    public void fallBackToAnotherCollectionBecauseOfAttributesAndTTL() {
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollections(List.of(
                        getCollection(1000, Set.of("a", "b", "c")),
                        getCollection(2000, Set.of("a", "b", "c")),
                        getCollection(10000, Set.of("a", "b")),
                        getCollection(20000, Set.of("a"))
                ))
                .build();
        Bucket bucket = getRandomBucket();
        bucket.setBegin(5000);
        bucket.getAttributes().put("a", "1");
        bucket.getAttributes().put("b", "2");
        bucket.getAttributes().put("c", "3");
        bucket.getAttributes().put("d", "4");
        timeSeries.getDefaultCollection().getIngestionPipeline().ingestBucket(bucket);
        timeSeries.getCollections().forEach(c -> c.getIngestionPipeline().flush());



    }

}

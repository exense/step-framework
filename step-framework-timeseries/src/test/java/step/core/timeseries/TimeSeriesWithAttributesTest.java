package step.core.timeseries;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import step.core.collections.Filters;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.*;
import java.util.stream.Collectors;

@RunWith(JUnitParamsRunner.class)
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
        randomBucket.getAttributes().put("z", "custom");
        ingestionPipeline.ingestBucket(randomBucket);
        ingestionPipeline.flush();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("z"))
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        Map<Long, Bucket> buckets = response.getSeries().get(Map.of("z", "custom"));
        Assert.assertNotNull(buckets);
        Assert.assertNotNull(buckets.get(0L));
        Assert.assertTrue(response.isTtlCovered());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("a"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        buckets = response.getSeries().get(Map.of());
        Assert.assertNotNull(buckets);
        Bucket bucket = collection.getCollection().find(Filters.empty(), null, null, null, 0).collect(Collectors.toList()).get(0);
        Assert.assertNull(bucket.getAttributes().get("a"));
        Assert.assertNotNull(bucket.getAttributes().get("z"));
    }

    @Test
    public void simpleCollectionWithFilteringAndGroupingAttributes() {
        TimeSeriesCollection collection = getCollection(1000, Set.of("a", "b", "c"));
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();
        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        Bucket randomBucket = getRandomBucket();
        randomBucket.setBegin(1);
        randomBucket.getAttributes().put("a", "valueA"); // ignored
        randomBucket.getAttributes().put("b", "valueB"); // ignored
        randomBucket.getAttributes().put("c", "valueC"); // ignored
        randomBucket.getAttributes().put("y", "valueY");
        randomBucket.getAttributes().put("z", "valueZ");
        ingestionPipeline.ingestBucket(randomBucket);
        ingestionPipeline.flush();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("z"))
                .withFilter(Map.of("y", "valueY"))
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(1, response.getFirstSeries().size());
        Map<Long, Bucket> buckets = response.getSeries().get(Map.of("z", "valueZ"));
        Assert.assertNotNull(buckets);
        Assert.assertNotNull(buckets.get(0L));

        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3000)
                .withGroupDimensions(Set.of("a"))
                .withFilter(Map.of("a", "valueA"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(0, response.getSeries().size());
    }

    @Test
    public void fallBackToAnotherCollectionBecauseOfAttributes() {
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollections(List.of(
                        getCollection(1000, Set.of()),
                        getCollection(2000, Set.of("a")),
                        getCollection(10000, Set.of("a", "b")),
                        getCollection(20000, Set.of("a", "b", "c"))
                ))
                .build();
        Bucket bucket = getRandomBucket();
        bucket.setAttributes(new BucketAttributes());
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
            c.getIgnoredAttributes().forEach(attr -> Assert.assertFalse(foundBucket.getAttributes().containsKey(attr)));
            Assert.assertEquals(bucket.getAttributes().size() - c.getIgnoredAttributes().size(), foundBucket.getAttributes().size());
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
                .withGroupDimensions(Set.of("b"))
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
        Assert.assertEquals(10_000, response.getCollectionResolution());
//
        query = new TimeSeriesAggregationQueryBuilder()
                .range(0, AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1) // to make sure 3rd interval is chosen
                .withFilter(Map.of("c", "3"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(10_000, response.getCollectionResolution()); // first collection
    }

    @Test
    public void fallBackToAnotherCollectionBecauseOfAttributesAndTTL() {
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollections(List.of(
                        getCollectionWithTTL(1000, 1000L, Set.of()),
                        getCollectionWithTTL(2000, 2000L, Set.of("a")),
                        getCollectionWithTTL(10000, 5000L, Set.of("a", "b")),
                        getCollectionWithTTL(20000, 10_000L, Set.of("a", "b", "c"))
                ))
                .setTtlEnabled(true)
                .build();
        Bucket bucket = getRandomBucket();
        long now = System.currentTimeMillis();
        bucket.setBegin(now - 500);
        bucket.getAttributes().put("a", "1");
        bucket.getAttributes().put("b", "2");
        bucket.getAttributes().put("c", "3");
        bucket.getAttributes().put("d", "4");
        timeSeries.getDefaultCollection().getIngestionPipeline().ingestBucket(bucket);
        timeSeries.getCollections().forEach(c -> c.getIngestionPipeline().flush());

        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 500, now)
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(1000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 1500, now)
                // no filter
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertTrue(response.isTtlCovered());
        Assert.assertEquals(2000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 1500, now)
                .withFilter(Map.of("a", "1"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());
        Assert.assertEquals(1000, response.getCollectionResolution()); // because a is excluded in second resolution

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1, now)
                .withFilter(Map.of("c", "3"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());
        Assert.assertEquals(10000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1, now)
                .withGroupDimensions(Set.of("b"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());
        Assert.assertEquals(2000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1, now)
                .withGroupDimensions(Set.of("a", "b"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());
        Assert.assertEquals(1000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1, now)
                .withGroupDimensions(Set.of("b", "c"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());
        Assert.assertEquals(2000, response.getCollectionResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - AGGREGATION_RESOLUTION_LAMBDA * 10_000 + 1, now)
                .withGroupDimensions(Set.of("a", "z"))
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertFalse(response.isTtlCovered());
        Assert.assertEquals(1000, response.getCollectionResolution());

    }

    @Parameters(method = "validAttributesData")
    @Test
    public void validAttributes(List<Set<String>> collectionsAttributes) {
        List<TimeSeriesCollection> collections = new ArrayList<>();
        for (int i = 0; i < collectionsAttributes.size(); i++) {
            collections.add(getCollection((long) Math.pow(2, i), collectionsAttributes.get(i)));
        }
        new TimeSeriesBuilder()
                .registerCollections(collections)
                .build();
    }

    @Parameters(method = "invalidAttributesData")
    @Test(expected = IllegalArgumentException.class)
    public void invalidAttributes(List<Set<String>> collectionsAttributes) {
        List<TimeSeriesCollection> collections = new ArrayList<>();
        for (int i = 0; i < collectionsAttributes.size(); i++) {
            collections.add(getCollection((long) Math.pow(2, i), collectionsAttributes.get(i)));
        }
        new TimeSeriesBuilder()
                .registerCollections(collections)
                .build();
    }


    private static Object[] validAttributesData() {
        return new Object[]{
                List.of(Set.of("a", "b", "c"), Set.of("a", "b", "c"), Set.of("a", "b", "c")),
                List.of(Set.of(), Set.of("a", "b", "c"), Set.of("a", "b", "c", "d")),
                List.of(Set.of(), Set.of(), Set.of("a", "b")),
                List.of(Set.of(), Set.of(), Set.of()),
                List.of(Set.of("a", "b"), Set.of("a", "b"), Set.of("a", "b" ,"c")),

        };
    }

    private static Object[] invalidAttributesData() {
        return new Object[]{
                List.of( Set.of("a", "b"), Set.of("a", "b", "c"), Set.of("a", "b")),
                List.of(Set.of("a", "b", "c"), Set.of("a", "b"), Set.of()),
                List.of(Set.of(), Set.of("a", "b"), Set.of()),
                List.of(Set.of(), Set.of("a", "b"), Set.of("a", "c")),
                List.of(Set.of("a"), Set.of("b")),

        };
    }

}

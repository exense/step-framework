package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;

public class TimeSeriesMultipleCollectionsAggregateTest extends TimeSeriesBaseTest {

    @Test
    public void testChosenResolutionBasedOnRange() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(1000, 5000, 10_000);
        Bucket b1 = getRandomBucket();
        long now = System.currentTimeMillis();
        b1.setBegin(now);
        timeSeries.getDefaultCollection().save(b1);
        timeSeries.ingestDataForEmptyCollections();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        //Requested range 10_000_000 -> select coarser resolution
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 10_000 * 1000, now)
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(10_000, response.getCollectionResolution());
        //Requested range 5000 -> select finest resolution
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 5000, now)
                .build();
        response = aggregationPipeline.collect(query);
        //Requested range 1000 -> select finest resolution
        Assert.assertEquals(1000, response.getCollectionResolution());
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 1000, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(1000, response.getCollectionResolution());
        //Requested range 500_001 -> select resolution closed to 100 buckets (1000 -> 500 buckets, 5000 -> 100 buckest (winner))
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 5000 * AGGREGATION_RESOLUTION_LAMBDA + 1, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(5000, response.getCollectionResolution());
        //Requested range 499_999 -> select resolution closed to 100 buckets (1000 -> 500 buckets, 5000 -> 100 buckest (winner))
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 5000 * AGGREGATION_RESOLUTION_LAMBDA-1, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(5000, response.getCollectionResolution());
        //Requested range 1_000_001 -> select resolution closed to 100 buckets (5000 -> 200 buckets, 10000 -> 100 buckest (winner))
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 10_000 * AGGREGATION_RESOLUTION_LAMBDA + 1, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(10_000, response.getCollectionResolution());
        query = new TimeSeriesAggregationQueryBuilder()
                .range(now - 10_000 * AGGREGATION_RESOLUTION_LAMBDA, now)
                .build();
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(10_000, response.getCollectionResolution());
    }

    @Test
    public void testChosenResolutionBasedOnTTL() {
        TimeSeries timeSeries = getTimeSeriesWithResolutions(1000, 5000, 10_000);
        long now = System.currentTimeMillis();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        long from = now - 1000 * AGGREGATION_RESOLUTION_LAMBDA;
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(from, now) // first resolution should be ideal
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
        Assert.assertEquals(1000, response.getCollectionResolution());
        timeSeries.getCollections().get(0).setTtl(10_000);
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(5000, response.getCollectionResolution());
        timeSeries.getCollections().get(0).setTtl(100_000_000);
        response = aggregationPipeline.collect(query);
        Assert.assertEquals(1000, response.getCollectionResolution());
    }




}

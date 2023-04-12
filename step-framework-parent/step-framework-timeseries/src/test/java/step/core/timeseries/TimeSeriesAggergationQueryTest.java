package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.ql.OQLFilterBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TimeSeriesAggergationQueryTest {

    @Test(expected = IllegalArgumentException.class)
    public void lowInvalidResolutionTest() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 10);
        timeSeries.getAggregationPipeline()
                .newQueryBuilder()
                .window(9);
    }

    @Test
    public void lowValidResolutionTest() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 10);
        timeSeries.getAggregationPipeline()
                .newQueryBuilder()
                .window(10);
    }


    @Test
    public void shrinkTest() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        long bucketSize = pipeline.newQueryBuilder()
                .split(1)
                .build()
                .getBucketSize();
        Assert.assertEquals(Long.MAX_VALUE, bucketSize);
    }

    @Test
    public void emptyFiltersTest() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 1);
        String oql = null;
        Map<String, String> params = null;
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        long seriesSize = pipeline.newQueryBuilder()
                .split(1)
                .withFilter(OQLFilterBuilder.getFilter(oql))
                .build()
                .run().getSeries().size();
        // we want to make sure that the methods above are not failing
        Assert.assertEquals(0, seriesSize);
    }

    @Test
    public void queryIntervalPrecisionTest() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        int bucketsCount = 20;
        for (int i = 0; i < bucketsCount; i++) {
            Bucket bucket = new Bucket();
            bucket.setBegin(i * 1000);
            bucketCollection.save(bucket);
        }
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 200);
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        String oql = "";
        int split = 5;
        TimeSeriesAggregationQuery query = pipeline.newQueryBuilder()
                .range(0, 1000 * bucketsCount / 2)
                .split(split)
                .withFilter(OQLFilterBuilder.getFilter(oql))
                .build();
        TimeSeriesAggregationResponse response = query.run();
        // we want to make sure that the methods above are not failing
        Assert.assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> seriesResponse = response.getFirstSeries();
        Assert.assertEquals(split, seriesResponse.size());
    }

}

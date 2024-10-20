package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.ql.OQLFilterBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TimeSeriesAggergationQueryTest extends TimeSeriesBaseTest {

    @Test(expected = IllegalArgumentException.class)
    public void lowInvalidResolutionTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .window(9)
                .build();
        aggregationPipeline.collect(query);
    }

    @Test
    public void lowValidResolutionTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .window(10)
                .range(0, 10000)
                .build();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(query);
    }


    @Test
    public void shrinkTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(1)
                .build();
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        Assert.assertTrue(response.getResolution() > System.currentTimeMillis() - 3_000);
        response.getSeries().values().forEach(map -> {
            Assert.assertTrue(map.values().size() <= 1);
        });
    }

    @Test
    public void emptyFiltersTest() {
        TimeSeries timeSeries = getNewTimeSeries(10);
        String oql = null;
        Map<String, String> params = null;
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(1)
                .withFilter(OQLFilterBuilder.getFilter(oql))
                .build();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        // we want to make sure that the methods above are not failing
        Assert.assertEquals(0, response.getSeries().size());
    }

    @Test
    public void queryIntervalPrecisionTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        int bucketsCount = 20;
        for (int i = 0; i < bucketsCount; i++) {
            Bucket bucket = new Bucket();
            bucket.setBegin(i * 1000);
            timeSeries.getDefaultCollection().getCollection().save(bucket);
        }
        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        String oql = "";
        int split = 5;
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 1000 * bucketsCount / 2)
                .split(split)
                .withFilter(OQLFilterBuilder.getFilter(oql))
                .build();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        Assert.assertEquals(1, response.getSeries().size());
        Map<Long, Bucket> seriesResponse = response.getFirstSeries();
        Assert.assertEquals(split, seriesResponse.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noRangeWithBucketsCountTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .split(5)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooBigSplitTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        long now = System.currentTimeMillis();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, now)
                .split(TimeSeriesAggregationPipeline.MAX_INTERVALS_IN_RESPONSE + 1)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallWindowTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        long now = System.currentTimeMillis();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - TimeSeriesAggregationPipeline.MAX_INTERVALS_IN_RESPONSE * 1001, now)
                .window(1000)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

}

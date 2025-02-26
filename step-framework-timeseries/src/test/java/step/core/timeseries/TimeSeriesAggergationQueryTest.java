package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.ql.OQLFilterBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

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
        Assert.assertEquals(0, response.getStart());
        Assert.assertEquals(10_000, response.getEnd());
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
        Assert.assertEquals(0, response.getStart());
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
            bucket.setBegin(i * 1000); // one every second
            timeSeries.getDefaultCollection().save(bucket);
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
        Assert.assertEquals(0, response.getStart());
        Assert.assertEquals(10_000, response.getEnd());
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
        TimeSeriesCollection collection = getCollection(200);
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollection(collection)
                .setSettings(new TimeSeriesSettings().setResponseMaxIntervals(1000))
                .build();
        long now = System.currentTimeMillis();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();

        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, now)
                .split(aggregationPipeline.getResponseMaxIntervals() + 1)
                .build();

        aggregationPipeline.collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallWindowTest() {
        TimeSeriesCollection collection = getCollection(200);
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollection(collection)
                .setSettings(new TimeSeriesSettings().setResponseMaxIntervals(1000))
                .build();
        long now = System.currentTimeMillis();
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(now - aggregationPipeline.getResponseMaxIntervals() * 1001L, now)
                .window(1000)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidIntervalTest() {
        TimeSeries timeSeries = getNewTimeSeries(200);
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(10000, 1000)
                .window(1000)
                .build();
        timeSeries.getAggregationPipeline().collect(query);
    }

    @Test
    public void responseRangeWithCustomWindowTest() {
        TimeSeries timeSeries = getNewTimeSeries(1000);
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(2000, 10_000)
                .window(5000) // should end up in 2 buckets
                .build();
        TimeSeriesAggregationResponse response = timeSeries.getAggregationPipeline().collect(query);
        Assert.assertEquals(2000, response.getStart());
        Assert.assertEquals(12_000, response.getEnd());
        Assert.assertEquals(5000, response.getResolution());

        query = new TimeSeriesAggregationQueryBuilder()
                .range(2000, 9500)
                .window(5000) // should end up in 2 buckets
                .build();
        response = timeSeries.getAggregationPipeline().collect(query);
        Assert.assertEquals(2000, response.getStart());
        Assert.assertEquals(12_000, response.getEnd());
        Assert.assertEquals(5000, response.getResolution());
    }

}

package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.bucket.Bucket;

import java.util.Set;

public class TimeSeriesAggregationQueryBuilderTest {

    @Test
    public void emptyQueryTest() {
        TimeSeriesAggregationQuery query = createPipeline(1000)
                .newQueryBuilder()
                .build();
        Assert.assertEquals(1000, query.getResolution());
        Assert.assertNull(query.getFrom());
        Assert.assertNull(query.getTo());
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowWithNoRangeTest() {
        TimeSeriesAggregationQuery query = createPipeline(1000)
                .newQueryBuilder()
                .window(200)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooLowWindowTest() {
        TimeSeriesAggregationQuery query = createPipeline(1000)
                .newQueryBuilder()
                .range(1000, 5000)
                .window(800)
                .build();
    }

    @Test
    public void perfectRangeRoundingTest() {
        TimeSeriesAggregationQuery query = createPipeline(1000)
                .newQueryBuilder()
                .range(1000, 5000)
                .build();
        Assert.assertEquals(1000, query.getResolution());
        Assert.assertEquals(1000, query.getFrom().longValue());
        Assert.assertEquals(5000, query.getTo().longValue());
    }

    /**
     * The window interval should be rounded to the smallest source resolution multiplier.
     */
    @Test()
    public void windowRoundingTest() {
        TimeSeriesAggregationQuery query = createPipeline(1000)
                .newQueryBuilder()
                .range(1000, 5000)
                .window(1200)
                .build();
        Assert.assertEquals(1000, query.getResolution());

        query = createPipeline(1000)
                .newQueryBuilder()
                .range(1000, 5000)
                .window(1900) // should round to 1000
                .build();
        Assert.assertEquals(1000, query.getResolution());

        query = createPipeline(1000)
                .newQueryBuilder()
                .range(1000, 5000)
                .window(2100) // should round to 2000
                .build();
        Assert.assertEquals(2000, query.getResolution());
    }

    @Test
    public void simpleShrinkTest() {
        TimeSeriesAggregationQuery query = createPipeline(1000)
                .newQueryBuilder()
                .range(800, 5200)
                .split(1) // shrink
                .build();
        Assert.assertEquals(Long.MAX_VALUE, query.getResolution());
        Assert.assertEquals(0, query.getFrom().longValue());
        Assert.assertEquals(6000, query.getTo().longValue());
    }

    @Test
    public void enoughSplitTest() {
        TimeSeriesAggregationQuery query = createPipeline(500)
                .newQueryBuilder()
                .range(800, 5500) // should be transformed to 500-5500
                .split(5)
                .build();
        Assert.assertEquals(1000, query.getResolution());
        Assert.assertEquals(500, query.getFrom().longValue());
        Assert.assertEquals(5500, query.getTo().longValue());

        query = createPipeline(500)
                .newQueryBuilder()
                .range(800, 6000) // should be transformed to 500-6000
                .split(5)
                .build();
        Assert.assertEquals(1000, query.getResolution());
        Assert.assertEquals(500, query.getFrom().longValue());
        Assert.assertEquals(6000, query.getTo().longValue());
    }

    @Test
    public void insufficientSplitTest() {
        TimeSeriesAggregationQuery query = createPipeline(500)
                .newQueryBuilder()
                .range(4000, 5000) // there is not enough space for 4 minimum buckets
                .split(4)
                .build();
        Assert.assertEquals(500, query.getResolution());
        Assert.assertEquals(4000, query.getFrom().longValue());
        Assert.assertEquals(5000, query.getTo().longValue());

        query = createPipeline(500)
                .newQueryBuilder()
                .range(4000, 5100) // there is not enough space for 4 minimum buckets
                .split(5)
                .build();
        Assert.assertEquals(500, query.getResolution());
        Assert.assertEquals(4000, query.getFrom().longValue());
        Assert.assertEquals(5500, query.getTo().longValue());

        query = createPipeline(300)
                .newQueryBuilder()
                .range(1000, 2000) // there is not enough space for 4 minimum buckets
                .split(15)
                .build();
        Assert.assertEquals(300, query.getResolution());
        Assert.assertEquals(900, query.getFrom().longValue());
        Assert.assertEquals(2100, query.getTo().longValue());

        query = createPipeline(500)
                .newQueryBuilder()
                .range(1000, 2600) // there is not enough space for 4 minimum buckets
                .split(3)
                .build();
        Assert.assertEquals(500, query.getResolution());
        Assert.assertEquals(1000, query.getFrom().longValue());
        Assert.assertEquals(3000, query.getTo().longValue());

        query = createPipeline(500)
                .newQueryBuilder()
                .range(0, 2900) // there is not enough space for 4 minimum buckets
                .split(3)
                .build();
        Assert.assertEquals(1000, query.getResolution());
        Assert.assertEquals(0, query.getFrom().longValue());
        Assert.assertEquals(3000, query.getTo().longValue());
    }

    /**
     * Split must take precedence
     */
    @Test
    public void splitVsResolutionPriorityTest() {
        TimeSeriesAggregationQuery query = createPipeline(300)
                .newQueryBuilder()
                .range(1000, 3000) // 900 to 3000 = 3900
                .window(1000)
                .split(4)
                .build();
        Assert.assertEquals(600, query.getResolution());
        Assert.assertEquals(900, query.getFrom().longValue());
        Assert.assertEquals(3000, query.getTo().longValue());
    }

    private static TimeSeriesAggregationPipeline createPipeline(int sourceResolution) {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, sourceResolution);
        return timeSeries.getAggregationPipeline();
    }

}

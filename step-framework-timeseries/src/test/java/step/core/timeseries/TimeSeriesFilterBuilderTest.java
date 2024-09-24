package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filter;
import step.core.collections.filters.And;
import step.core.collections.filters.Equals;
import step.core.collections.filters.True;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.query.TimeSeriesQuery;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.HashMap;
import java.util.Map;

public class TimeSeriesFilterBuilderTest {

    @Test
    public void buildOqlFilterTest() {
        Filter filter = TimeSeriesFilterBuilder.buildFilter("field1 = abc");
        Assert.assertTrue(filter instanceof Equals);
        Assert.assertTrue(CollectionUtils.isEmpty(filter.getChildren()));
        filter = TimeSeriesFilterBuilder.buildFilter("");
        Assert.assertTrue(filter instanceof True);
    }

    @Test
    public void buildParamsFilterTest() {
        Map<String, Object> params = new HashMap<>();
        Filter filter = TimeSeriesFilterBuilder.buildFilter(params);
        Assert.assertTrue(filter instanceof True);
        Assert.assertTrue(CollectionUtils.isEmpty(filter.getChildren()));
        params.put("field1", "abc");
        params.put("field2", true);
        params.put("field3", 15);
        params.put("field4", 15L);
        filter = TimeSeriesFilterBuilder.buildFilter(params);
        Assert.assertTrue(filter instanceof And);
        Assert.assertEquals(4, filter.getChildren().size());
    }

    @Test
    public void buildTimestampFilterTest() {
        Filter filter = TimeSeriesFilterBuilder.buildFilter(10L, 20L);
        Assert.assertTrue(filter instanceof And);
        Assert.assertEquals(2, filter.getChildren().size());
    }

    @Test
    public void buildTimeSeriesQueryFilterTest() {
        TimeSeriesQuery query = new TimeSeriesQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter("field1 = abc"))
                .range(10L, 20L)
                .build();
        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        Assert.assertTrue(filter instanceof And);
        Assert.assertEquals(2, filter.getChildren().size());
    }

    @Test
    public void buildTimeSeriesAggregationQueryFilterTest() {
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .withFilter(TimeSeriesFilterBuilder.buildFilter("field1 = abc"))
                .range(10L, 20L)
                .build();
        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        Assert.assertTrue(filter instanceof And);
        Assert.assertEquals(2, filter.getChildren().size());
    }

}

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
import step.core.timeseries.query.OQLTimeSeriesFilterBuilder;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class TimeSeriesOQLTest {

    @Test
    public void oqlAttributesTest() {
        String oql = "field1 = 5 and field2 = 4 and (field3 < 5 or field3 > 15)";
        Set<String> attributes = new HashSet<>(OQLTimeSeriesFilterBuilder.getFilterAttributes(oql));
        Assert.assertTrue(attributes.containsAll(Arrays.asList("field1", "field2", "field3")));
        Assert.assertEquals(3, attributes.size());
    }

    @Test
    public void oqlTestWithoutFilter() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeriesCollection collection = new TimeSeriesCollection(bucketCollection, 1);
        TimeSeries timeSeries = new TimeSeries(Arrays.asList(collection));

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3)
                .withGroupDimensions(Set.of("status"))
                .withFilter(OQLFilterBuilder.getFilter("attributes.name = t1"))
                .build();
        TimeSeriesAggregationResponse response = pipeline.collect(query);

        assertEquals(2, response.getSeries().size());
    }

    @Test
    public void oqlTestWithFilter() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeriesCollection collection = new TimeSeriesCollection(bucketCollection, 1);
        TimeSeries timeSeries = new TimeSeries(Arrays.asList(collection));

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationQuery query = new TimeSeriesAggregationQueryBuilder()
                .range(0, 3)
                .withGroupDimensions(Set.of("status"))
                .withFilter(OQLFilterBuilder.getFilter("attributes.status = FAILED and attributes.name = t1"))
                .build();
        TimeSeriesAggregationResponse response = pipeline.collect(query);
        assertEquals(1, response.getSeries().size());
    }

}

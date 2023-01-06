package step.core.timeseries;

import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TimeSeriesOQLTests {

    @Test
    public void oqlTestWithoutFilter() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.newQuery()
                .range(0, 3)
                .groupBy(Set.of("status"))
                .filter("attributes.name = t1")
                .run();
        assertEquals(2, response.getSeries().size());
    }

    @Test
    public void oqlTestWithFilter() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 1);

        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
        }

        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = pipeline.newQuery()
                .range(0, 3)
                .groupBy(Set.of("status"))
                .filter(Map.of("status", "FAILED"))
                .filter("attributes.name = t1")
                .run();
        assertEquals(1, response.getSeries().size());
    }

}

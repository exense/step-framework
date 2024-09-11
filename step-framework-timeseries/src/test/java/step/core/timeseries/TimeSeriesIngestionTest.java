package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQueryBuilder;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.Map;
import java.util.Set;

public class TimeSeriesIngestionTest {

    @Test
    public void testMergeIngestion() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        int resolution = 1000;
        TimeSeriesCollectionSettings settings = new TimeSeriesCollectionSettings()
                .setMergeBucketsOnIngestionFlush(true)
                .setResolution(resolution);
        TimeSeriesCollection collection = new TimeSeriesCollection(bucketCollection, settings);
        TimeSeries timeSeries = new TimeSeriesBuilder().registerCollection(collection).build();

        long bucketValue1 = 5;
        long bucketValue2 = 50;

        TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.getIngestionPipeline();

        for (int i = 0; i < 5; i++) {
            Map<String, Object> object1 = Map.of("key", "value1");
            Map<String, Object> object2 = Map.of("key", "value2");
            ingestionPipeline.ingestPoint(object1, i * 100, bucketValue1);
            ingestionPipeline.ingestPoint(object2, i * 100, bucketValue2);
            ingestionPipeline.flush(); // force saving between objects
        }
        TimeSeriesAggregationPipeline aggregationPipeline = timeSeries.getAggregationPipeline();
        TimeSeriesAggregationResponse response = aggregationPipeline.collect(new TimeSeriesAggregationQueryBuilder()
                        .withGroupDimensions(Set.of("key"))
                        .range(0, 2000)
                .build());
        Assert.assertEquals(response.getResolution(), resolution);
        Assert.assertEquals(2, response.getSeries().size());
        Assert.assertEquals(2, response.getAxis().size());
        Assert.assertEquals(2, timeSeries.getDefaultCollection().getCollection().count(Filters.empty(), null));
    }

}

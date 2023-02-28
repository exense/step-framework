//package step.core.timeseries;
//
//import org.junit.Assert;
//import org.junit.Test;
//import step.core.collections.inmemory.InMemoryCollection;
//
//import java.util.Map;
//import java.util.Set;
//
//import static org.junit.Assert.assertEquals;
//
//public class TimeSeriesAggergationQueryTest {
//
//    @Test(expected = IllegalArgumentException.class)
//    public void lowInvalidResolutionTest() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 10);
//        timeSeries.getAggregationPipeline()
//                .newQuery()
//                .window(9);
//    }
//
//    @Test
//    public void lowValidResolutionTest() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 10);
//        timeSeries.getAggregationPipeline()
//                .newQuery()
//                .window(10);
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void intervalNotSetTest() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 10);
//        timeSeries.getAggregationPipeline()
//                .newQuery()
//                .split(10);
//    }
//
//    @Test
//    public void shrinkTest() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 1);
//
//        try (TimeSeriesIngestionPipeline ingestionPipeline = timeSeries.newIngestionPipeline()) {
//            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "PASSED"), 1L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "t1", "status", "FAILED"), 2L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "PASSED"), 1L, 10L);
//            ingestionPipeline.ingestPoint(Map.of("name", "t2", "status", "FAILED"), 2L, 10L);
//        }
//
//        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
//        long bucketSize = pipeline.newQuery()
//                .split(1)
//                .getBucketSize();
//        Assert.assertEquals(Long.MAX_VALUE, bucketSize);
//    }
//
//    @Test
//    public void emptyFiltersTest() {
//        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
//        TimeSeries timeSeries = new TimeSeries(bucketCollection, Set.of(), 1);
//        String oql = null;
//        Map<String, String> params = null;
//        TimeSeriesAggregationPipeline pipeline = timeSeries.getAggregationPipeline();
//        long seriesSize = pipeline.newQuery()
//                .split(1)
//                .filter(oql)
//                .filter(params)
//                .run().getSeries().size();
//        // we want to make sure that the methods above are not failing
//        Assert.assertEquals(0, seriesSize);
//
//    }
//
//}

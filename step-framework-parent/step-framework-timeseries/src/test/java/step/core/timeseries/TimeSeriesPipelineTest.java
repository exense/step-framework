package step.core.timeseries;

import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TimeSeriesPipelineTest {

    @Test
    public void test() {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeriesPipeline pipeline = new TimeSeriesPipeline(new BucketAccessor(bucketCollection));

        long nPoints = 100000L;
        for (int i = 0; i < nPoints; i++) {
            Bucket entity = new Bucket(1000L * i);
            entity.setCount(1);
            entity.setSum(5);
            entity.setMin(-i - 1);
            entity.setMax(i + 1);
            bucketCollection.save(entity);
        }

        Map<Long, Bucket> query = pipeline.query(Map.of(), 0L, nPoints * 1000, 1000);
        assertEquals(nPoints, query.size());

        query = pipeline.query(Map.of(), 0L, nPoints * 1000, nPoints * 1000);
        assertEquals(1, query.size());
        Bucket bucket = query.get(0L);
        assertEquals(nPoints, bucket.getCount());
        assertEquals(nPoints * 5, bucket.getSum());
        assertEquals(-nPoints, bucket.getMin());
        assertEquals(nPoints, bucket.getMax());

        query = pipeline.query(Map.of(), 0L, nPoints * 1000, 2000);
        assertEquals(nPoints / 2, query.size());
        assertEquals(2, query.get(0L).getCount());
    }
}
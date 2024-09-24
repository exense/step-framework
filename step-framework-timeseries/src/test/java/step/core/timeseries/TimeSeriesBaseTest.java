package step.core.timeseries;

import org.junit.Assert;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesBaseTest {

    protected Random rand = new Random();

    protected TimeSeriesCollection getCollection(long resolution) {
        InMemoryCollection<Bucket> col = new InMemoryCollection<>();
        return new TimeSeriesCollection(col, resolution);
    }

    protected void assertAllCollectionsAreEmpty(TimeSeries ts) {
        ts.getCollections().forEach(c -> Assert.assertEquals(0, c.getCollection().count(Filters.empty(), null)));
    }

    protected TimeSeries getTimeSeriesWithResolutions(long... resolutions) {
        return new TimeSeriesBuilder()
                .registerCollections(Arrays.stream(resolutions).mapToObj(this::getCollection).collect(Collectors.toList()))
                .build();
    }

    protected TimeSeries getNewTimeSeries(long resolution) {
        InMemoryCollection<Bucket> bucketCollection = new InMemoryCollection<>();
        TimeSeriesCollection collection = new TimeSeriesCollection(bucketCollection, resolution);
        return new TimeSeriesBuilder()
                .registerCollection(collection)
                .build();
    }

    protected Bucket getRandomBucket() {
        Bucket bucket = new Bucket();
        bucket.setBegin(1);
        bucket.setCount(rand.nextInt(1000));
        bucket.setMax(rand.nextInt(1000) + 1000);
        bucket.setMin(rand.nextInt(1000));
        bucket.setAttributes(new BucketAttributes(Map.of("key", "value")));
        return bucket;
    }

}

package step.core.timeseries;

import org.junit.Assert;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesBaseTest {

    /**
     * This represents the minimum number of intervals in a requested range. If the requested range contains more intervals, a resolution above will be used instead.
     */
    protected long AGGREGATION_RESOLUTION_LAMBDA = 100;
    protected Random rand = new Random();

    protected TimeSeriesCollection getCollection(long resolution) {
        return getCollection(resolution, null);
    }

    protected TimeSeriesCollection getCollection(long resolution, Set<String> attributes) {
        InMemoryCollection<Bucket> col = new InMemoryCollection<>();
        TimeSeriesCollectionSettings settings = new TimeSeriesCollectionSettings()
                .setResolution(resolution)
                .setHandledAttributes(attributes);
        return new TimeSeriesCollection(col, settings);
    }

    protected TimeSeriesCollection getCollectionWithTTL(long resolution, long ttl) {
        return getCollectionWithTTL(resolution, ttl, null);
    }

    protected TimeSeriesCollection getCollectionWithTTL(long resolution, long ttl, Set<String> attributes) {
        InMemoryCollection<Bucket> col = new InMemoryCollection<>();
        TimeSeriesCollectionSettings settings = new TimeSeriesCollectionSettings()
                .setTtl(ttl)
                .setResolution(resolution)
                .setHandledAttributes(attributes);
        return new TimeSeriesCollection(col, settings);
    }

    protected void assertAllCollectionsAreEmpty(TimeSeries ts) {
        ts.getCollections().forEach(this::assertCollectionIsEmpty);
    }

    protected void assertCollectionIsEmpty(TimeSeriesCollection c) {
        Assert.assertEquals(0, c.getCollection().count(Filters.empty(), null));
    }

    protected void assertCollectionsAreEmpty(Collection<TimeSeriesCollection> collections) {
        collections.forEach(this::assertCollectionIsEmpty);
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
        bucket.setSum(rand.nextInt(1000));
        bucket.setCount(rand.nextInt(1000));
        bucket.setMax(rand.nextInt(1000) + 1000);
        bucket.setMin(rand.nextInt(1000));
        bucket.setAttributes(new BucketAttributes(Map.of("key", "value")));
        return bucket;
    }

}

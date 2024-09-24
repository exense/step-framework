package step.core.timeseries;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.bucket.Bucket;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(JUnitParamsRunner.class)
public class TimeSeriesMultipleCollectionsTest {

    private TimeSeriesCollection getCollection(long resolution) {
        InMemoryCollection<Bucket> col = new InMemoryCollection<>();
        return new TimeSeriesCollection(col, resolution);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "invalidResolutionsData")
    public void invalidMultipleResolutionsTest(List<Long> resolutions) {
        List<TimeSeriesCollection> collections = resolutions.stream().map(r -> getCollection(r)).collect(Collectors.toList());
        new TimeSeriesBuilder()
                .registerCollections(collections)
                .build();
    }

    @Test
    public void hasCollectionCheckTest() {
        List<Long> resolutions = Arrays.asList(10L, 20L, 40L, 80L, 160L, 320L);
        TimeSeries timeSeries = new TimeSeriesBuilder()
                .registerCollections(resolutions.stream().map(this::getCollection).collect(Collectors.toList()))
                .build();
        resolutions.forEach(r -> Assert.assertTrue(timeSeries.hasCollection(r)));
        Assert.assertFalse(timeSeries.hasCollection(0));
        Assert.assertFalse(timeSeries.hasCollection(9));
        Assert.assertFalse(timeSeries.hasCollection(11));
        Assert.assertFalse(timeSeries.hasCollection(90));
        Assert.assertFalse(timeSeries.hasCollection(200));
        timeSeries.close();
    }

    private static Object[] invalidResolutionsData() {
        return new Object[]{
                Arrays.asList(100L, 200L, 300L),
                Arrays.asList(100L, 200L, 401L),
                Arrays.asList(100L, 100L),
                Arrays.asList(100L, 201L),
                Arrays.asList(-100L, 200L),
                Arrays.asList(-200L, -100L),
                Arrays.asList(0L),
                Arrays.asList(100L, 110L, 120L),

        };
    }


}

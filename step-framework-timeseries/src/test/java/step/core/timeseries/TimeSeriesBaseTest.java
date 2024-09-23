package step.core.timeseries;

import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.bucket.Bucket;

public class TimeSeriesBaseTest {

    protected TimeSeriesCollection getCollection(long resolution) {
        InMemoryCollection<Bucket> col = new InMemoryCollection<>();
        return new TimeSeriesCollection(col, resolution);
    }

}

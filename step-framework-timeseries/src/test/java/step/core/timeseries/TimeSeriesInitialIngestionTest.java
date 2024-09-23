package step.core.timeseries;

import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.bucket.Bucket;

import java.util.Arrays;
import java.util.List;

public class TimeSeriesInitialIngestionTest extends TimeSeriesBaseTest {


    public void ingestDataForEmptyCollections() {
        List<TimeSeriesCollection> collections = Arrays.asList(
                getCollection(100),
                getCollection(200),
                getCollection(400),
                getCollection(800),
                getCollection(1600)
        );
    }


}

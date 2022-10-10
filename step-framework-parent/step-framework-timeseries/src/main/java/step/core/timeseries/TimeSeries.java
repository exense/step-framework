package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;

import java.util.Set;

public class TimeSeries {

    private final Collection<Bucket> collection;
    private final Set<String> indexedFields;
    private final Integer timeSeriesResolution;

    public TimeSeries(Collection<Bucket> collection, Set<String> indexedAttributes, Integer timeSeriesResolution) {
        this.collection = collection;
        this.indexedFields = indexedAttributes;
        this.timeSeriesResolution = timeSeriesResolution;
        createIndexes();

    }

    public TimeSeries(CollectionFactory collectionFactory, String collectionName, Set<String> indexedAttributes, Integer ingestionResolutionPeriod) {
        this(collectionFactory.getCollection(collectionName, Bucket.class), indexedAttributes, ingestionResolutionPeriod);
    }

    private void createIndexes() {
        collection.createOrUpdateIndex("begin");
        indexedFields.forEach(f -> collection.createOrUpdateIndex("attributes."+f));
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline() {
        return new TimeSeriesIngestionPipeline(collection, timeSeriesResolution);
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline(long flushingPeriodInMs) {
        return new TimeSeriesIngestionPipeline(collection, timeSeriesResolution, flushingPeriodInMs);
    }

    public TimeSeriesAggregationPipeline getAggregationPipeline() {
        return new TimeSeriesAggregationPipeline(collection, timeSeriesResolution);
    }

    protected static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }
}

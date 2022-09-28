package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;

import java.util.Set;

public class TimeSeries {

    private final Collection<Bucket> collection;
    private final Set<String> indexedFields;
    private final Integer ingestionResolutionPeriod;

    public TimeSeries(CollectionFactory collectionFactory, String collectionName, Set<String> indexedAttributes, Integer ingestionResolutionPeriod) {
        this.collection = collectionFactory.getCollection(collectionName, Bucket.class);
        this.indexedFields = indexedAttributes;
        this.ingestionResolutionPeriod = ingestionResolutionPeriod;
        createIndexes();
    }

    private void createIndexes() {
        collection.createOrUpdateIndex("begin");
        indexedFields.forEach(f -> collection.createOrUpdateIndex("attributes."+f));
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline(long flushingPeriodInMs) {
        return newIngestionPipeline(this.ingestionResolutionPeriod, flushingPeriodInMs);
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline(long customResolutionMs, long flushingPeriodInMs) {
        return new TimeSeriesIngestionPipeline(collection, customResolutionMs, flushingPeriodInMs);
    }

    public TimeSeriesAggregationPipeline getAggregationPipeline(int resolution) {
        return new TimeSeriesAggregationPipeline(collection, resolution);
    }
}

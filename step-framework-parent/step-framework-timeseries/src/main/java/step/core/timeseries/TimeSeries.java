package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;

import java.util.Set;

public class TimeSeries {

    private final Collection<Bucket> collection;
    private final Set<String> indexedFields;

    public TimeSeries(CollectionFactory collectionFactory, String collectionName, Set<String> indexedAttributes) {
        this.collection = collectionFactory.getCollection(collectionName, Bucket.class);
        this.indexedFields = indexedAttributes;
        createIndexes();
    }

    private void createIndexes() {
        collection.createOrUpdateIndex("begin");
        indexedFields.forEach(f -> collection.createOrUpdateIndex("attributes."+f));
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline(long resolutionInMs, long flushingPeriodInMs) {
        return new TimeSeriesIngestionPipeline(collection, resolutionInMs, flushingPeriodInMs);
    }

    public TimeSeriesAggregationPipeline getAggregationPipeline(int resolution) {
        return new TimeSeriesAggregationPipeline(collection, resolution);
    }
}

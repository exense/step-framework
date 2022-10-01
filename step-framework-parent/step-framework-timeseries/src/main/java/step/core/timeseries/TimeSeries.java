package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;

import java.util.Set;
import java.util.function.Function;

public class TimeSeries {

    private final Collection<Bucket> collection;
    private final Set<String> indexedFields;
    private final Integer ingestionResolutionPeriod;

    private final Function<Long,Function<Long, Long>> indexProjectionFunctionFactory;

    public TimeSeries(Collection<Bucket> collection, Set<String> indexedAttributes, Integer timeSeriesResolution) {
        this.collection = collection;
        this.indexedFields = indexedAttributes;
        this.ingestionResolutionPeriod = timeSeriesResolution;
        this.indexProjectionFunctionFactory = this::timestampToIndex;
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
        return new TimeSeriesIngestionPipeline(collection, ingestionResolutionPeriod, indexProjectionFunctionFactory);
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline(long flushingPeriodInMs) {
        return new TimeSeriesIngestionPipeline(collection, ingestionResolutionPeriod, flushingPeriodInMs, indexProjectionFunctionFactory);
    }

    public TimeSeriesAggregationPipeline getAggregationPipeline() {
        return new TimeSeriesAggregationPipeline(collection, ingestionResolutionPeriod, indexProjectionFunctionFactory);
    }

    private Function<Long, Long> timestampToIndex(long resolution) {
        return t ->  t - t % resolution;
    }
}

package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.Filter;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.query.TimeSeriesQuery;

import java.util.*;

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

    public void performHousekeeping(TimeSeriesQuery housekeepingQuery) {
        Filter filter = TimeSeriesFilterBuilder.buildFilter(housekeepingQuery);
        collection.remove(filter);
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

    public static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }

}

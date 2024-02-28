package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.Filter;
import step.core.collections.IndexField;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.query.TimeSeriesQuery;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeries {

    private final Collection<Bucket> collection;
    private final Integer timeSeriesResolution;

    public TimeSeries(Collection<Bucket> collection,  Integer timeSeriesResolution) {
        this.collection = collection;
        this.timeSeriesResolution = timeSeriesResolution;
    }

    public TimeSeries(CollectionFactory collectionFactory, String collectionName, Integer ingestionResolutionPeriod) {
        this(collectionFactory.getCollection(collectionName, Bucket.class), ingestionResolutionPeriod);
    }

    /**
     * Create default TimeSeries indexes and single field indexes for the custom ones passed in argument
     * @param indexFields the set of single field indexes to be created
     */
    public void createIndexes(Set<IndexField> indexFields) {
        collection.createOrUpdateIndex("begin");
        Set<IndexField> renamedFieldIndexes = indexFields.stream().map(i -> new IndexField("attributes." + i.fieldName,
                i.order, i.fieldClass)).collect(Collectors.toSet());
        renamedFieldIndexes.forEach(collection::createOrUpdateIndex);
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

package step.core.timeseries;

import step.core.collections.IndexField;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.*;

public class TimeSeries {

    private final List<TimeSeriesCollection> handledCollections;
    private Map<Long, TimeSeriesCollection> collectionsByResolution = new TreeMap<>();
    private TimeSeriesAggregationPipeline aggregationPipeline;
    

    TimeSeries(List<TimeSeriesCollection> handledCollections) {
        this.handledCollections = handledCollections;
        handledCollections.forEach(c -> collectionsByResolution.put(c.getResolution(), c));
        aggregationPipeline = new TimeSeriesAggregationPipeline(handledCollections);
    }
    
    public TimeSeriesIngestionPipeline getIngestionPipeline() {
        return this.handledCollections.get(0).getIngestionPipeline();
    }
    
    public TimeSeriesAggregationPipeline getAggregationPipeline() {
        return aggregationPipeline;
    }
    
    /**
     * Create default TimeSeries indexes and single field indexes for the custom ones passed in argument
     * @param indexFields the set of single field indexes to be created
     */
    public void createIndexes(Set<IndexField> indexFields) {
        this.handledCollections.forEach(c -> c.createIndexes(indexFields));
    }

//    public void performHousekeeping(TimeSeriesQuery housekeepingQuery) {
//        Filter filter = TimeSeriesFilterBuilder.buildFilter(housekeepingQuery);
//        collection.remove(filter);
//    }

//    public TimeSeriesIngestionPipeline newIngestionPipeline() {
//        return new TimeSeriesIngestionPipeline(collection, timeSeriesResolution);
//    }
//
//    public TimeSeriesIngestionPipeline newIngestionPipeline(long flushingPeriodInMs) {
//        return new TimeSeriesIngestionPipeline(collection, timeSeriesResolution, flushingPeriodInMs);
//    }
//
//    public TimeSeriesAggregationPipeline getAggregationPipeline() {
//        return new TimeSeriesAggregationPipeline(collection, timeSeriesResolution);
//    }
//
    public static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }

}

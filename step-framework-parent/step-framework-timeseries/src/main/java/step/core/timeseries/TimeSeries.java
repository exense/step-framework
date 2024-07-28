package step.core.timeseries;

import step.core.collections.IndexField;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;

import java.util.*;

public class TimeSeries {

    private List<TimeSeriesCollection> handledCollections;
    private Map<Long, TimeSeriesCollection> collectionsByResolution = new TreeMap<>();
    

    TimeSeries(List<TimeSeriesCollection> handledCollections) {
        this.handledCollections = handledCollections;
        handledCollections.forEach(c -> collectionsByResolution.put(c.getResolution(), c));
    }
    
    public TimeSeriesCollection getMainCollection() {
        return handledCollections.get(0);
    }
    
    public List<Long> getAvailableResolutions() {
		return new ArrayList<>(collectionsByResolution.keySet());
	}
    
    public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
		handledCollections.get(0).ingestPoint(attributes, timestamp, value);
	}
    
    public TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        long adjustedResolution = roundRequiredResolution(query.getResolution());
        TimeSeriesCollection timeSeriesCollection = collectionsByResolution.get(adjustedResolution);
        return timeSeriesCollection.collect(query);
    }
    
    private long roundRequiredResolution(long targetResolution) {
        List<Long> availableResolutions = getAvailableResolutions();
        for (int i = 1; i < availableResolutions.size(); i++) {
            if (availableResolutions.get(i) > targetResolution) {
                return availableResolutions.get(i - 1);
            }
        }
        return availableResolutions.get(availableResolutions.size() - 1); // last resolution
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

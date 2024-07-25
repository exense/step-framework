package step.core.timeseries;

import step.core.collections.Filter;
import step.core.collections.IndexField;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.ingestion.TimeSeriesIngestionChain;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.query.TimeSeriesQuery;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeries {

    private TimeSeriesIngestionChain ingestionChain;

    public TimeSeries(TimeSeriesIngestionChain ingestionChain) {
        this.ingestionChain = ingestionChain;
    }
    
    public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
		ingestionChain.ingestPoint(attributes, timestamp, value);
	}
    
    public TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        long adjustedResolution = roundRequiredResolution(query.getResolution());
    }
    
    private long roundRequiredResolution(long targetResolution) {
        List<Long> availableResolutions = ingestionChain.getAvailableResolutions();
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
//    public void createIndexes(Set<IndexField> indexFields) {
//        collection.createOrUpdateIndex("begin");
//        Set<IndexField> renamedFieldIndexes = indexFields.stream().map(i -> new IndexField("attributes." + i.fieldName,
//                i.order, i.fieldClass)).collect(Collectors.toSet());
//        renamedFieldIndexes.forEach(collection::createOrUpdateIndex);
//    }

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

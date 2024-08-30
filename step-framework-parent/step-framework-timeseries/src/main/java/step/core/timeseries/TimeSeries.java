package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.IndexField;
import step.core.collections.SearchOrder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.query.TimeSeriesQuery;

import java.util.*;

import static step.core.timeseries.TimeSeriesConstants.TIMESTAMP_ATTRIBUTE;

public class TimeSeries {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeries.class);

    private final List<TimeSeriesCollection> handledCollections;
    private Map<Long, TimeSeriesCollection> collectionsByResolution = new TreeMap<>();
    private TimeSeriesAggregationPipeline aggregationPipeline;
    

    TimeSeries(List<TimeSeriesCollection> handledCollections) {
        this.handledCollections = handledCollections;
        handledCollections.forEach(c -> collectionsByResolution.put(c.getResolution(), c));
        aggregationPipeline = new TimeSeriesAggregationPipeline(handledCollections);
    }

    public void createMissingData() {
        List<TimeSeriesCollection> sortedCollections = new ArrayList<>(collectionsByResolution.values());
        for (int i = 1; i < sortedCollections.size(); i++) {
            TimeSeriesCollection collection = sortedCollections.get(i);
            if (collection.isEmpty()) {
                String collectionName = collection.getCollection().getName();
                logger.debug("Populating time-series collection: " + collectionName);
                TimeSeriesCollection previousCollection = sortedCollections.get(i - 1);
                try (TimeSeriesIngestionPipeline ingestionPipeline = new TimeSeriesIngestionPipeline(collection.getCollection(), collection.getResolution(), 30000)) {
                    SearchOrder searchOrder = new SearchOrder(TIMESTAMP_ATTRIBUTE, 1);
                    Filter filter = collection.getTtl() > 0 ? Filters.gte("begin", System.currentTimeMillis() - collection.getTtl()): Filters.empty();
                    previousCollection.getCollection().findLazy(filter, searchOrder, null, null, 0).forEach(bucket -> {
                        ingestionPipeline.ingestBucket(bucket);
                    });
                    ingestionPipeline.flush();
                } catch (Exception e) {
                    logger.error("Error while populating {} collection. Dropping the entire collection...", collectionName, e);
                    collection.getCollection().drop();
                }
            }
        }
    }

    /**
     * @return the first (smallest resolution) collection in the chain
     */
    public TimeSeriesCollection getDefaultCollection() {
        return this.handledCollections.get(0);
    }

    public TimeSeriesCollection getCollection(long resolution) {
        return this.collectionsByResolution.get(resolution);
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

    /**
     * Perform standard housekeeping based on internal TTL settings
     */
    public void performHousekeeping() {
        this.handledCollections.forEach(TimeSeriesCollection::performHousekeeping);
    }

    /**
     * Perform custom housekeeping.
     */
    public void performHousekeeping(TimeSeriesQuery query) {
        this.handledCollections.forEach(collection -> collection.removeData(query));
    }

    public static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }

}

package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.IndexField;
import step.core.collections.SearchOrder;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipelineSettings;
import step.core.timeseries.query.TimeSeriesQuery;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static step.core.timeseries.TimeSeriesConstants.TIMESTAMP_ATTRIBUTE;

public class TimeSeries implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeries.class);

    private final List<TimeSeriesCollection> handledCollections;
    private final TimeSeriesAggregationPipeline aggregationPipeline;
    

    TimeSeries(List<TimeSeriesCollection> handledCollections) {
        this.handledCollections = handledCollections;
        aggregationPipeline = new TimeSeriesAggregationPipeline(handledCollections);
    }

    /**
     * This method will ingest the data for all the resolutions which are empty.
     * Each collection is ingesting the data from the previous collection only.
     * <p>
     * If this fails by any reason, the entire collection is dropped.
     */
    public void ingestDataForEmptyCollections() {
        for (int i = 1; i < handledCollections.size(); i++) {
            TimeSeriesCollection collection = handledCollections.get(i);
            if (collection.isEmpty()) {
                String collectionName = collection.getCollection().getName();
                logger.debug("Populating time-series collection: " + collectionName);
                TimeSeriesCollection previousCollection = handledCollections.get(i - 1);
                TimeSeriesIngestionPipelineSettings ingestionSettings = new TimeSeriesIngestionPipelineSettings()
                        .setResolution(collection.getResolution())
                        .setFlushingPeriodMs(TimeUnit.SECONDS.toMillis(30));
                try (TimeSeriesIngestionPipeline ingestionPipeline = new TimeSeriesIngestionPipeline(collection.getCollection(), ingestionSettings)) {
                    SearchOrder searchOrder = new SearchOrder(TIMESTAMP_ATTRIBUTE, 1);
                    Filter filter = collection.getTtl() > 0 ? Filters.gte("begin", System.currentTimeMillis() - collection.getTtl()): Filters.empty();

                    try (Stream<Bucket> bucketStream = previousCollection
                            .getCollection()
                            .findLazy(filter, searchOrder, null, null, 0)) {

                        bucketStream.forEach(ingestionPipeline::ingestBucket);
                        ingestionPipeline.flush();
                    }
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

    public boolean hasCollection(long resolution) {
        return handledCollections.stream().anyMatch(c -> c.getResolution() == resolution);
    }

    public TimeSeriesCollection getCollection(long resolution) {
        return handledCollections
                .stream()
                .filter(collection -> collection.getResolution() == resolution)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Collection with resolution not found " + resolution));
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

    public List<TimeSeriesCollection> getCollections() {
        return handledCollections;
    }

    @Override
    public void close() {
        this.handledCollections.forEach(c -> c.getIngestionPipeline().close());
    }
}

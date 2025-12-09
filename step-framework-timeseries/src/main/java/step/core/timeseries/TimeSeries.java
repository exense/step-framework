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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static step.core.timeseries.TimeSeriesConstants.TIMESTAMP_ATTRIBUTE;

public class TimeSeries implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeries.class);

    private final List<TimeSeriesCollection> handledCollections;
    private final TimeSeriesAggregationPipeline aggregationPipeline;

    private boolean ttlEnabled;

    TimeSeries(List<TimeSeriesCollection> handledCollections) {
        this(handledCollections, new TimeSeriesSettings());
    }

    TimeSeries(List<TimeSeriesCollection> handledCollections, TimeSeriesSettings settings) {
        this.handledCollections = handledCollections;
        this.ttlEnabled = settings.isTtlEnabled();
        aggregationPipeline = new TimeSeriesAggregationPipeline(handledCollections, settings.getResponseMaxIntervals(), settings.getIdealResponseIntervals(), settings.isTtlEnabled());
    }
    public boolean isTtlEnabled() {
        return ttlEnabled;
    }

    public TimeSeries setTtlEnabled(boolean ttlEnabled) {
        this.ttlEnabled = ttlEnabled;
        this.aggregationPipeline.setTtlEnabled(ttlEnabled);
        return this;
    }

    public void updateAllCollectionsTtl(boolean ttlEnabled, Map<Long, Long> resolutionsToTtl) {
        setTtlEnabled(ttlEnabled);
        resolutionsToTtl.forEach((resolution, ttl) -> {
            if (hasCollection(resolution)) {
                getCollection(resolution).setTtl(ttl);
            }
        });
    }
    public void updateCollectionTtl(long resolution, long ttl) {
        if (hasCollection(resolution)) {
            getCollection(resolution).setTtl(ttl);
        }
    }

    /**
     * This method will ingest the data for all the resolutions which are empty.
     * Each collection is ingesting the data from the previous collection only.
     * <p>
     * If this fails by any reason, the entire collection is dropped.
     */
    public void ingestDataForEmptyCollections() {
        long controllerStart = System.currentTimeMillis();
        logger.info("Configured collections: {}", handledCollections.stream().map(TimeSeriesCollection::getName).collect(Collectors.toList()));
        //This is run in as an async task; since data can be flushed in between, we need to get the list of empty collections before starting processing any of them
        List<TimeSeriesCollection> emptyCollections = handledCollections.stream().filter(TimeSeriesCollection::isEmpty).collect(Collectors.toList());
        logger.warn("Empty collections detected: {}, do not stop the controller until it's re-ingestion is completed.", emptyCollections.stream().map(TimeSeriesCollection::getName).collect(Collectors.toList()));
        for (int i = 1; i < handledCollections.size(); i++) {
            TimeSeriesCollection collection = handledCollections.get(i);
            if (emptyCollections.contains(collection)) {
                String collectionName = collection.getName();
                logger.info("Populating empty time-series collection: " + collectionName);
                TimeSeriesCollection previousCollection = handledCollections.get(i - 1);
                TimeSeriesIngestionPipelineSettings ingestionSettings = new TimeSeriesIngestionPipelineSettings()
                        .setIgnoredAttributes(collection.getIgnoredAttributes())
                        .setResolution(collection.getResolution())
                        .setFlushingPeriodMs(TimeUnit.SECONDS.toMillis(30))
                        .setFlushAsyncQueueSize(5000);
                try (TimeSeriesIngestionPipeline ingestionPipeline = new TimeSeriesIngestionPipeline(collection, ingestionSettings)) {
                    SearchOrder searchOrder = new SearchOrder(TIMESTAMP_ATTRIBUTE, 1);
                    Filter afterStart = Filters.lt("begin", controllerStart);
                    Filter filter = collection.getTtl() > 0 ?
                            Filters.and(List.of(afterStart, Filters.gte("begin", System.currentTimeMillis() - collection.getTtl())))
                            : afterStart;
                    try (Stream<Bucket> bucketStream = previousCollection.findLazy(filter, searchOrder)) {
                        bucketStream.forEach(ingestionPipeline::ingestBucket);
                        ingestionPipeline.flush();
                    }
                } catch (Throwable e) {
                    logger.error("Error while populating {} collection. Dropping the entire collection...", collectionName, e);
                    collection.drop();
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
     * Create compound index using the given fields
     * @param indexFields the set of fields to use for creating a compound index
     */
    public void createCompoundIndex(LinkedHashSet<IndexField> indexFields) {
        this.handledCollections.forEach(c -> c.createCompoundIndex(indexFields));
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

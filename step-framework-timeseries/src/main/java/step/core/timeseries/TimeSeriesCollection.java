package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.*;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.aggregation.TimeSeriesProcessedParams;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.query.TimeSeriesQuery;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static step.core.timeseries.TimeSeriesConfig.DEFAULT_INGESTION_FLUSH_SERIES_QUEUE_SIZE;


public class TimeSeriesCollection {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCollection.class);

    private final Collection<Bucket> mainCollection;
    private final long resolutionMs;
    private final TimeSeriesIngestionPipeline ingestionPipeline;
    private long ttlMs; // In milliseconds. set to 0 in case deletion is never required
    private final Set<String> ignoredAttributes;

    public TimeSeriesCollection(Collection<Bucket> mainCollection, long resolutionMs) {
        this(mainCollection, new TimeSeriesCollectionConfig()
            .setResolutionMs(resolutionMs)
            .setIngestionFlushSeriesQueueSize(DEFAULT_INGESTION_FLUSH_SERIES_QUEUE_SIZE)
        );
    }

    public TimeSeriesCollection(Collection<Bucket> mainCollection, TimeSeriesCollectionConfig settings) {
        this.mainCollection = Objects.requireNonNull(mainCollection);
        if (settings.getResolutionMs() <= 0) {
            throw new IllegalArgumentException("The resolution parameter must be greater than zero");
        }
        this.resolutionMs = settings.getResolutionMs();
        validateTtl(settings.getTtlMs());
        this.ttlMs = settings.getTtlMs();
        this.ingestionPipeline = new TimeSeriesIngestionPipeline(this, settings);
        this.ignoredAttributes = settings.getIgnoredAttributes();
    }

    public Collection<Bucket> getUnderlyingCollection() {
        return mainCollection;
    }

    /**
     * This method check if the collection isEmpty in the backend database.
     * Currently used to detect if the collection needs to be rebuilds based on higher resolution collection
     *
     * @return true if empty
     */
    protected boolean isEmpty() {
        return mainCollection.estimatedCount() <= 0;
    }

    /**
     * This is the one used by the aggregation pipeline querying both the data from the ingestion pipeline (i.e. not yet flushed)
     * and the data already persisted
     *
     * @param queryParameters the query parameters
     * @return a stream of buckets matching the query parameters
     */
    public Stream<Bucket> queryTimeSeries(TimeSeriesProcessedParams queryParameters) {
        InMemoryCollection<Bucket> inMemoryCollection = ingestionPipeline.getCurrenStateToInMemoryCollection(queryParameters.getTo());
        Filter filter = TimeSeriesFilterBuilder.buildFilter(queryParameters);
        return Stream.concat(inMemoryCollection.findLazy(filter, null, null, null, 0),
            mainCollection.findLazy(filter, null, null, null, 0));
    }

    protected void performHousekeeping() {
        if (ttlMs > 0) {
            long cleanupRangeStart = 0;
            long cleanupRangeEnd = System.currentTimeMillis() - ttlMs;
            TimeSeriesQuery query = new TimeSeriesQueryBuilder().range(cleanupRangeStart, cleanupRangeEnd).build();
            Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
            this.mainCollection.remove(filter);
            logger.debug("Housekeeping successfully performed for collection {}", this.mainCollection.getName());
        }
    }

    protected void removeData(TimeSeriesQuery query) {
        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        this.mainCollection.remove(filter);
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public void setTtlMs(long ttlInMs) {
        validateTtl(ttlInMs);
        this.ttlMs = ttlInMs;
    }

    private void validateTtl(long ttl) {
        if (ttl < 0) {
            throw new IllegalArgumentException("Negative ttl value is not allowed");
        }
    }

    /**
     * Creates separate indexes on the given attributes.
     *
     * @param indexFields attribute names to be indexed (one index per attribute)
     */
    protected void createIndexes(Set<IndexField> indexFields) {
        mainCollection.createOrUpdateIndex(TimeSeriesConstants.TIMESTAMP_ATTRIBUTE);
        Set<IndexField> renamedFieldIndexes = indexFields.stream().map(i -> new IndexField("attributes." + i.fieldName,
            i.order, i.fieldClass)).collect(Collectors.toSet());
        renamedFieldIndexes.forEach(mainCollection::createOrUpdateIndex);
    }

    /**
     * Creates a compound index on the underlying raw collection.
     *
     * @param indexFields fields to create a compound index on
     */
    public void createCompoundIndex(LinkedHashSet<IndexField> indexFields) {
        mainCollection.createOrUpdateCompoundIndex(indexFields);
    }

    public TimeSeriesIngestionPipeline getIngestionPipeline() {
        return ingestionPipeline;
    }

    public long getResolutionMs() {
        return resolutionMs;
    }

    public Set<String> getIgnoredAttributes() {
        return ignoredAttributes;
    }

    public String getName() {
        return mainCollection.getName();
    }

    /**
     * Only used by Junit to check persisted bucket count
     *
     * @param filter query filter
     * @param limit  maximum results fetched and returned (0 unlimited)
     * @return the count of matching object or the provided limit and if more objects matched
     */
    protected long count(Filter filter, Integer limit) {
        return mainCollection.count(filter, limit);
    }

    /**
     * Only used by Junit to check persisted data
     *
     * @param filter the filter of the query
     * @return a stream of matching buckets
     */
    protected Stream<Bucket> find(Filter filter) {
        //SearchOrder only support concatenation of inMemory and DB data if the field is the time.
        return mainCollection.find(filter, null, null, null, 0);
    }

    /**
     *
     * @param filter the filter of the query
     * @param order  the order of the query
     * @return a stream of matching buckets sorted by the provided order parameter
     */
    protected Stream<Bucket> findLazy(Filter filter, SearchOrder order) {
        return mainCollection.findLazy(filter, order, null, null, 0);
    }

    public Bucket save(Bucket entity) {
        return mainCollection.save(entity);
    }

    public void save(Iterable<Bucket> entities) {
        mainCollection.save(entities);
    }

    protected void drop() {
        mainCollection.drop();
    }
}

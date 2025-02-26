package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.*;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.aggregation.TimeSeriesProcessedParams;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipelineSettings;
import step.core.timeseries.query.TimeSeriesQuery;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TimeSeriesCollection extends AbstractCollection<Bucket> implements Collection<Bucket> {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCollection.class);

    private final Collection<Bucket> mainCollection;
    private final long resolution;
    private final TimeSeriesIngestionPipeline ingestionPipeline;
    private long ttl; // In milliseconds. set to 0 in case deletion is never required
    private final Set<String> ignoredAttributes;

    public TimeSeriesCollection(Collection<Bucket> mainCollection, long resolution) {
        this(mainCollection, new TimeSeriesCollectionSettings()
                .setResolution(resolution)
        );
    }

    public TimeSeriesCollection(Collection<Bucket> mainCollection, long resolution, long flushPeriod) {
        this(mainCollection, new TimeSeriesCollectionSettings()
                .setResolution(resolution)
                .setIngestionFlushingPeriodMs(flushPeriod)
        );
    }


    public TimeSeriesCollection(Collection<Bucket> mainCollection, TimeSeriesCollectionSettings settings) {
        if (settings.getResolution() <= 0) {
            throw new IllegalArgumentException("The resolution parameter must be greater than zero");
        }
        validateTtl(settings.getTtl());
        this.mainCollection = mainCollection;
        this.resolution = settings.getResolution();
        this.ttl = settings.getTtl();
        TimeSeriesIngestionPipelineSettings ingestionSettings = new TimeSeriesIngestionPipelineSettings()
                .setResolution(settings.getResolution())
                .setFlushingPeriodMs(settings.getIngestionFlushingPeriodMs())
                .setFlushAsyncQueueSize(settings.getIngestionFlushAsyncQueueSize())
                .setIgnoredAttributes(settings.getIgnoredAttributes());
        this.ingestionPipeline = new TimeSeriesIngestionPipeline(this, ingestionSettings);
        this.ignoredAttributes = settings.getIgnoredAttributes();
    }

    public boolean isEmpty() {
        return mainCollection.estimatedCount() == 0;
    }

    public Stream<Bucket> queryTimeSeries(TimeSeriesProcessedParams finalParams) {
        InMemoryCollection<Bucket> inMemoryCollection = ingestionPipeline.getCurrenStateToInMemoryCollection(finalParams.getTo());
        Filter filter = TimeSeriesFilterBuilder.buildFilter(finalParams);
        return Stream.concat(inMemoryCollection.findLazy(filter, null, null, null, 0),
                mainCollection.findLazy(filter, null, null, null, 0));
    }

    public void performHousekeeping() {
        if (ttl > 0) {
            long cleanupRangeStart = 0;
            long cleanupRangeEnd = System.currentTimeMillis() - ttl;
            TimeSeriesQuery query = new TimeSeriesQueryBuilder().range(cleanupRangeStart, cleanupRangeEnd).build();
            Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
            this.mainCollection.remove(filter);
            logger.debug("Housekeeping successfully performed for collection {}", this.mainCollection.getName());
        }
    }

    public void removeData(TimeSeriesQuery query) {
        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        this.mainCollection.remove(filter);
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttlInMs) {
        validateTtl(ttlInMs);
        this.ttl = ttlInMs;
    }

    private void validateTtl(long ttl) {
        if (ttl < 0) {
            throw new IllegalArgumentException("Negative ttl value is not allowed");
        }
    }

    public void createIndexes(Set<IndexField> indexFields) {
        mainCollection.createOrUpdateIndex(TimeSeriesConstants.TIMESTAMP_ATTRIBUTE);
        Set<IndexField> renamedFieldIndexes = indexFields.stream().map(i -> new IndexField("attributes." + i.fieldName,
                i.order, i.fieldClass)).collect(Collectors.toSet());
        renamedFieldIndexes.forEach(mainCollection::createOrUpdateIndex);
    }

    public TimeSeriesIngestionPipeline getIngestionPipeline() {
        return ingestionPipeline;
    }

    public long getResolution() {
        return resolution;
    }

    public Set<String> getIgnoredAttributes() {
        return ignoredAttributes;
    }

    @Override
    public String getName() {
        return mainCollection.getName();
    }

    @Override
    public long count(Filter filter, Integer limit) {
        return mainCollection.count(filter, limit);
    }

    @Override
    public long estimatedCount() {
        return mainCollection.estimatedCount();
    }

    @Override
    public Stream<Bucket> find(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
        //SearchOrder only support concatenation of inMemory and DB data if the field is the time.
        return mainCollection.find(filter, order, skip, limit, maxTime);
    }

    @Override
    public Stream<Bucket> findLazy(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
        //aggregation pipeline is using the queryTimeSeries function directly, here we directly query the main collection
        return mainCollection.findLazy(filter, order, skip, limit, maxTime);
    }

    @Override
    public Stream<Bucket> findReduced(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime, List<String> reduceFields) {
        return mainCollection.findReduced(filter, order, skip, limit, maxTime, reduceFields);
    }

    @Override
    public List<String> distinct(String columnName, Filter filter) {
        return mainCollection.distinct(columnName, filter);
    }

    @Override
    public void remove(Filter filter) {
        mainCollection.remove(filter);
    }

    @Override
    public Bucket save(Bucket entity) {
        return mainCollection.save(entity);
    }

    @Override
    public void save(Iterable<Bucket> entities) {
        mainCollection.save(entities);
    }

    @Override
    public void createOrUpdateIndex(String field) {
        mainCollection.createOrUpdateIndex(field);
    }

    @Override
    public void createOrUpdateIndex(IndexField indexField) {
        mainCollection.createOrUpdateIndex(indexField);
    }

    @Override
    public void createOrUpdateIndex(String field, Order order) {
        mainCollection.createOrUpdateIndex(field, order);
    }

    @Override
    public void createOrUpdateCompoundIndex(String... fields) {
        mainCollection.createOrUpdateCompoundIndex(fields);
    }

    @Override
    public void createOrUpdateCompoundIndex(LinkedHashSet<IndexField> fields) {
        mainCollection.createOrUpdateCompoundIndex(fields);
    }

    @Override
    public void rename(String newName) {
        mainCollection.rename(newName);
    }

    @Override
    public void drop() {
        mainCollection.drop();
    }

    @Override
    public Class<Bucket> getEntityClass() {
        return mainCollection.getEntityClass();
    }

    @Override
    public void dropIndex(String indexName) {
        mainCollection.dropIndex(indexName);
    }
}

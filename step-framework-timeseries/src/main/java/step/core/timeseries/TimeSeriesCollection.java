package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.IndexField;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipelineSettings;
import step.core.timeseries.query.TimeSeriesQuery;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.Set;
import java.util.stream.Collectors;


public class TimeSeriesCollection {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCollection.class);

    private final Collection<Bucket> collection;
    private final long resolution;
    private final TimeSeriesIngestionPipeline ingestionPipeline;
    private long ttl; // In milliseconds. set to 0 in case deletion is never required

    public TimeSeriesCollection(Collection<Bucket> collection, long resolution) {
        this(collection, new TimeSeriesCollectionSettings()
                .setResolution(resolution)
        );
    }

    public TimeSeriesCollection(Collection<Bucket> collection, long resolution, long flushPeriod) {
        this(collection, new TimeSeriesCollectionSettings()
                .setResolution(resolution)
                .setIngestionFlushingPeriodMs(flushPeriod)
        );
    }


    public TimeSeriesCollection(Collection<Bucket> collection, TimeSeriesCollectionSettings settings) {
        this.collection = collection;
        this.resolution = settings.getResolution();
        this.ttl = settings.getTtl();
        TimeSeriesIngestionPipelineSettings ingestionSettings = new TimeSeriesIngestionPipelineSettings()
                .setResolution(settings.getResolution())
                .setFlushingPeriodMs(settings.getIngestionFlushingPeriodMs());
        this.ingestionPipeline = new TimeSeriesIngestionPipeline(collection, ingestionSettings);
    }

    public boolean isEmpty() {
        return getCollection().estimatedCount() == 0;
    }

    public void performHousekeeping() {
        if (ttl > 0) {
            long cleanupRangeStart = 0;
            long cleanupRangeEnd = System.currentTimeMillis() - ttl;
            TimeSeriesQuery query = new TimeSeriesQueryBuilder().range(cleanupRangeStart, cleanupRangeEnd).build();
            Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
            this.collection.remove(filter);
            logger.debug("Housekeeping successfully performed for collection " + this.collection.getName());
        }
    }

    public void removeData(TimeSeriesQuery query) {
        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        this.collection.remove(filter);
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttlInMs) {
        if (ttlInMs < 0) {
            throw new IllegalArgumentException("Negative ttl value is not allowed");
        }
        this.ttl = ttlInMs;
    }

    public void createIndexes(Set<IndexField> indexFields) {
        collection.createOrUpdateIndex(TimeSeriesConstants.TIMESTAMP_ATTRIBUTE);
        Set<IndexField> renamedFieldIndexes = indexFields.stream().map(i -> new IndexField("attributes." + i.fieldName,
                i.order, i.fieldClass)).collect(Collectors.toSet());
        renamedFieldIndexes.forEach(collection::createOrUpdateIndex);
    }

    public Collection<Bucket> getCollection() {
        return collection;
    }

    public TimeSeriesIngestionPipeline getIngestionPipeline() {
        return ingestionPipeline;
    }

    public long getResolution() {
        return resolution;
    }

}

package step.core.timeseries.ingestion;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.async.AsyncProcessor;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.timeseries.TimeSeriesCollection;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimeSeriesIngestionPipeline implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIngestionPipeline.class);
    private static final long FLUSH_OFFSET = 10000; // buckets created in the last FLUSH_OFFSET ms will not be flushed.
    private static final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("timeseries-flush-%d").build();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final TimeSeriesCollection collection;
    private final long sourceResolution;
    private final ConcurrentHashMap<Long, Map<Map<String, Object>, BucketBuilder>> seriesQueue = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final LongAdder flushCount = new LongAdder();
    private final Set<String> ignoredAttributes;
    private TimeSeriesIngestionPipeline nextPipeline;
    private final AsyncProcessor<Bucket> asyncProcessor;
    private long lastFlush = 0;
    private final int seriesQueueSizeflush;

    public TimeSeriesIngestionPipeline(TimeSeriesCollection collection, TimeSeriesIngestionPipelineSettings settings) {
        validateSettings(settings);
        this.collection = collection;
        this.sourceResolution = settings.getResolution();
        long flushingPeriodMs = settings.getFlushingPeriodMs();
        if (flushingPeriodMs > 0) {
            scheduler = Executors.newScheduledThreadPool(1, threadFactory);
            scheduler.scheduleAtFixedRate(() -> flush(false), flushingPeriodMs, flushingPeriodMs, TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
        }
        this.ignoredAttributes = settings.getIgnoredAttributes();
        this.nextPipeline = settings.getNextPipeline();
        this.seriesQueueSizeflush = settings.getFlushSeriesQueueSize();
        //collection is null when overridden in TimeSeriesExecutionPlugin, in such case async processor is not required
        this.asyncProcessor =  (collection == null)  ? null : new AsyncProcessor<>(settings.getFlushAsyncQueueSize(), entity -> {
            try {
                collection.save(entity);
            } catch (Throwable e) {
                logger.error("Unable to save bucket with attribute {}", entity.getAttributes());
            }
        });
    }

    public void validateSettings(TimeSeriesIngestionPipelineSettings settings) {
        if (settings.getResolution() <= 0) {
            throw new IllegalArgumentException("The resolution parameter must be greater than zero");
        }
        if (settings.getFlushSeriesQueueSize() <= 1) {
            throw new IllegalArgumentException("The ingestion series queue size must be greater than 1");
        }
    }

    public long getResolution() {
        return sourceResolution;
    }

    public void ingestBucket(Bucket bucket) {
        if (logger.isTraceEnabled()) {
            logger.trace("Ingesting bucket");
        }
        lock.readLock().lock();
        try {
            long index = timestampToBucketTimestamp(bucket.getBegin(), sourceResolution);
            BucketAttributes bucketAttributes = removeIgnoredAttributes(bucket.getAttributes());
            Map<Map<String, Object>, BucketBuilder> bucketsForTimestamp = seriesQueue.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
            bucketsForTimestamp.computeIfAbsent(bucketAttributes, k ->
                            BucketBuilder.create(index).withAttributes(bucketAttributes))
                    .accumulate(bucket);
            if (nextPipeline != null) {
                nextPipeline.ingestBucket(bucket);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private BucketAttributes removeIgnoredAttributes(Map<String, Object> bucketAttributes) {
        BucketAttributes attributesCopy = new BucketAttributes(new HashMap<>(bucketAttributes));
        if (CollectionUtils.isNotEmpty(ignoredAttributes)) {
            ignoredAttributes.forEach(attributesCopy::remove);
        }
        return attributesCopy;
    }

    public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
        if (logger.isTraceEnabled()) {
            logger.trace("Ingesting point. Attributes=" + attributes.toString() + ", Timestamp=" + timestamp + ", Value=" + value);
        }
        BucketAttributes bucketAttributes = removeIgnoredAttributes(attributes);
        lock.readLock().lock();
        try {
            long index = timestampToBucketTimestamp(timestamp, sourceResolution);
            Map<Map<String, Object>, BucketBuilder> bucketsForTimestamp = seriesQueue.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
            bucketsForTimestamp.computeIfAbsent(bucketAttributes, k ->
                            BucketBuilder.create(index).withAttributes(bucketAttributes))
                    .ingest(value);
            if (nextPipeline != null) {
                nextPipeline.ingestPoint(attributes, timestamp, value);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Flush will be triggered only on the specified ingestion. Other pipelines in the chain will not be flushed.
     * This method forces the flush independently of the resolution or max queue size, it should be called for instance
     * once an execution completed.
     */
    public void flush() {
        flush(true);
    }

    /**
     *  Flush is performed only for the current ingestion pipeline (not the nested ones)
     *  It's usually only called directly by the schedule jobs flushing the time series on regular basis, it is also directly called for some Junit tests (located in different package)
     * @param forceFlush if false, the flush is only performed is the time interval is complete (i.e. bigger than the resolution) or if the queue size limit is reached
     */
    public void flush(boolean forceFlush) {
        lock.writeLock().lock();
        try {
            trace("Flushing");
            long now = System.currentTimeMillis();

            seriesQueue.forEach((k, v) -> {
                if (forceFlush || ((k + sourceResolution) < (now - FLUSH_OFFSET)) || v.size() >= seriesQueueSizeflush) {
                    // Remove the entry from the map and iterate over it afterwards
                    // This enables concurrent execution of flushing and ingestion
                    seriesQueue.remove(k).forEach((attributes, bucketBuilder) -> {
                        Bucket bucket = bucketBuilder.build();
                        if (forceFlush || asyncProcessor == null) {
                            collection.save(bucket);
                        } else {
                            asyncProcessor.enqueue(bucket);
                        }
                        lastFlush = System.currentTimeMillis();
                    });
                }
            });

            flushCount.increment();
            trace("Flushed");
        } catch (Throwable e) {
            logger.error("Error while flushing", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getFlushCount() {
        return flushCount.longValue();
    }

    public InMemoryCollection<Bucket> getCurrenStateToInMemoryCollection(long finalParamsTo) {
        InMemoryCollection<Bucket> buckets = new InMemoryCollection<>();
        if (finalParamsTo > lastFlush) {
            lock.readLock().lock();
            try {
                seriesQueue.values().stream().map(Map::values).flatMap(Collection::stream).map(BucketBuilder::build).forEach(buckets::save);
            } catch (Throwable e) {
                logger.error("Error while getting current pipeline data to inMemoryCollection", e);
            } finally {
                lock.readLock().unlock();
            }
        }
        return buckets;
    }

    private static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        flush();
        if (asyncProcessor != null) {
            try {
                asyncProcessor.close();
            } catch (Exception e) {
                logger.error("Unable to gracefully stop the bucket asynchronous persistence.", e);
            }
        }
    }

    private void trace(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace(message);
        }
    }

    public TimeSeriesIngestionPipeline setNextPipeline(TimeSeriesIngestionPipeline nextPipeline) {
        this.nextPipeline = nextPipeline;
        return this;
    }
}

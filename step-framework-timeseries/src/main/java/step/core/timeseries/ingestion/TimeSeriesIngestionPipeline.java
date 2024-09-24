package step.core.timeseries.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.filters.Equals;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TimeSeriesIngestionPipeline implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIngestionPipeline.class);
    private static final long FLUSH_OFFSET = 10000; // buckets created in the last FLUSH_OFFSET ms will not be flushed.

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Collection<Bucket> collection;
    private final long sourceResolution;
    private final ConcurrentHashMap<Long, Map<Map<String, Object>, BucketBuilder>> seriesQueue = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final LongAdder flushCount = new LongAdder();

    private TimeSeriesIngestionPipeline nextPipeline;

    public TimeSeriesIngestionPipeline(Collection<Bucket> collection, TimeSeriesIngestionPipelineSettings settings) {
        this.collection = collection;
        this.sourceResolution = settings.getResolution();
        long flushingPeriodMs = settings.getFlushingPeriodMs();
        if (flushingPeriodMs > 0) {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> flush(false), flushingPeriodMs, flushingPeriodMs, TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
        }
        this.nextPipeline = settings.getNextPipeline();
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
            Map<Map<String, Object>, BucketBuilder> bucketsForTimestamp = seriesQueue.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
            bucketsForTimestamp.computeIfAbsent(bucket.getAttributes(), k ->
                            BucketBuilder
                                    .create(index)
                                    .withAttributes(bucket.getAttributes()))
                    .accumulate(bucket);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
        if (logger.isTraceEnabled()) {
            logger.trace("Ingesting point. Attributes=" + attributes.toString() + ", Timestamp=" + timestamp + ", Value=" + value);
        }
        lock.readLock().lock();
        try {
            long index = timestampToBucketTimestamp(timestamp, sourceResolution);
            Map<Map<String, Object>, BucketBuilder> bucketsForTimestamp = seriesQueue.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
            bucketsForTimestamp.computeIfAbsent(attributes, k -> BucketBuilder.create(index).withAttributes(new BucketAttributes(attributes))).ingest(value);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Flush will be triggered only on the specified ingestion. Other pipelines in the chain will not be flushed.
     */
    public void flush() {
        flush(true);
    }

    private void flush(boolean forceFlush) {
        lock.writeLock().lock();
        try {
            trace("Flushing");
            long now = System.currentTimeMillis();

            seriesQueue.forEach((k, v) -> {
                if (forceFlush || k + sourceResolution < now - FLUSH_OFFSET) {
                    // Remove the entry from the map and iterate over it afterwards
                    // This enables concurrent execution of flushing and ingestion
                    seriesQueue.remove(k).forEach((attributes, bucketBuilder) -> {
                        Bucket bucket = bucketBuilder.build();
                        collection.save(bucket);
                        if (nextPipeline != null) {
                            nextPipeline.ingestBucket(bucket);
                        }
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

    private static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        flush();
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

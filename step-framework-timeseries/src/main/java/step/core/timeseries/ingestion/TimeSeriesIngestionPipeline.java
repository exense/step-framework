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
import java.util.function.Consumer;

public class TimeSeriesIngestionPipeline implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIngestionPipeline.class);
    private static final long FLUSH_OFFSET = 10000; // buckets created in the last FLUSH_OFFSET ms will not be flushed.

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Collection<Bucket> collection;
    private final long sourceResolution;
    private final ConcurrentHashMap<Long, Map<Map<String, Object>, BucketBuilder>> seriesQueue = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final LongAdder flushCount = new LongAdder();
    private Consumer<Bucket> flushCallback;
    private final boolean mergeBucketsOnFlush;

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
        this.mergeBucketsOnFlush = settings.isMergeBucketsOnFlush();
    }

//    public TimeSeriesIngestionPipeline(Collection<Bucket> collection, long resolution) {
//        this.collection = collection;
//        this.sourceResolution = resolution;
//        this.scheduler = null;
//        this.flushCallback = null;
//    }
//
//
//    public TimeSeriesIngestionPipeline(Collection<Bucket> collection, long resolution, long flushingPeriodInMs) {
//        this.collection = collection;
//        this.sourceResolution = resolution;
//        scheduler = Executors.newScheduledThreadPool(1);
//        scheduler.scheduleAtFixedRate(() -> flush(false), flushingPeriodInMs, flushingPeriodInMs, TimeUnit.MILLISECONDS);
//    }
//
    public TimeSeriesIngestionPipeline setFlushCallback(Consumer<Bucket> flushCallback) {
        this.flushCallback = flushCallback;
        return this;
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

    // TODO flush method can be improved with a batcher (run when a specific timeout ends, or a specific amount of data is collected)
    public void flush() {
        flush(true);
    }

    private void flush(boolean flushAll) {
        lock.writeLock().lock();
        try {
            trace("Flushing");
            long now = System.currentTimeMillis();

            seriesQueue.forEach((k, v) -> {
                if (flushAll || k < now - FLUSH_OFFSET) {
                    // Remove the entry from the map and iterate over it afterwards
                    // This enables concurrent execution of flushing and ingestion
                    seriesQueue.remove(k).forEach((attributes, bucketBuilder) -> {
                        Bucket bucket = bucketBuilder.build();
                        saveOrMerge(bucket);
                        if (flushCallback != null) {
                            flushCallback.accept(bucket);
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

    private void saveOrMerge(Bucket newBucket) {
        if (mergeBucketsOnFlush) {
            Equals beginFilter = Filters.equals("begin", newBucket.getBegin());
            Filter attributesFilter = getAttributesFilter(newBucket.getAttributes());
            Optional<Bucket> foundBucket = collection.find(Filters.and(Arrays.asList(beginFilter, attributesFilter)), null, null, null, 0).findAny();
            if (foundBucket.isPresent()) {
                Bucket existingBucket = foundBucket.get();
                Bucket accumulatedBucket = new BucketBuilder(existingBucket.getBegin())
                        .accumulate(existingBucket)
                        .accumulate(newBucket)
                        .build();
                accumulatedBucket.setId(existingBucket.getId());
                collection.save(accumulatedBucket);
                return;
            }
        }
        collection.save(newBucket);
    }

    private Filter getAttributesFilter(BucketAttributes attributes) {
        attributes.keySet().stream().map(key -> {
            Object value = attributes.get(key);
            if (value instanceof Boolean) {
                return Filters.equals(key, (boolean) value);
            } else if (value instanceof String) {
                return Filters.equals(key, (boolean) value);
            }
            else throw new IllegalArgumentException(value.getClass().getSimpleName() + " is not a supported value type");
        });
        return null;
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
}

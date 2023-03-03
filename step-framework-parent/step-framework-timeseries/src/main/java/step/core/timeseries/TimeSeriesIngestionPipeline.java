package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimeSeriesIngestionPipeline implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIngestionPipeline.class);
    private static final long OFFSET = 10000;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Collection<Bucket> collection;
    private final ConcurrentHashMap<Long, Map<BucketAttributes, BucketBuilder>> seriesQueue = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final LongAdder flushCount = new LongAdder();

    private final long sourceResolution;

    protected TimeSeriesIngestionPipeline(Collection<Bucket> collection, long resolutionInMs) {
        this.collection = collection;
        this.scheduler = null;
        this.sourceResolution = resolutionInMs;
    }

    protected TimeSeriesIngestionPipeline(Collection<Bucket> collection, long resolutionInMs, long flushingPeriodInMs) {
        this.collection = collection;
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> flush(false), flushingPeriodInMs, flushingPeriodInMs, TimeUnit.MILLISECONDS);
        this.sourceResolution = resolutionInMs;
    }

    public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
        ingestPoint(new BucketAttributes(attributes), timestamp, value);
    }

    public void ingestPoint(BucketAttributes attributes, long timestamp, long value) {
        if (logger.isTraceEnabled()) {
            logger.trace("Ingesting point. Attributes=" + attributes.toString() + ", Timestamp=" + timestamp + ", Value=" + value);
        }
        lock.readLock().lock();
        try {
            long index = TimeSeries.timestampToBucketTimestamp(timestamp, sourceResolution);
            Map<BucketAttributes, BucketBuilder> bucketsForTimestamp = seriesQueue.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
            bucketsForTimestamp.computeIfAbsent(attributes, k -> BucketBuilder.create(index).withAttributes(attributes)).ingest(value);
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
            debug("Flushing");
            long now = System.currentTimeMillis();

            seriesQueue.forEach((k, v) -> {
                if (flushAll || k < now - OFFSET) {
                    // Remove the entry from the map and iterate over it afterwards
                    // This enables concurrent execution of flushing and ingestion
                    seriesQueue.remove(k).forEach((attributes, bucketBuilder) -> collection.save(bucketBuilder.build()));
                }
            });

            flushCount.increment();
            debug("Flushed");
        } catch (Throwable e) {
            logger.error("Error while flushing", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getFlushCount() {
        return flushCount.longValue();
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        flush();
    }

    private void debug(String message) {
        if (logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }
}

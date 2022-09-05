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

public class TimeSeriesIngestionPipeline implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIngestionPipeline.class);

    private final Collection<Bucket> collection;
    private final Map<Long, Map<BucketAttributes, BucketBuilder>> seriesQueue = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final LongAdder flushCount = new LongAdder();
    private final long resolutionInMs;

    protected TimeSeriesIngestionPipeline(Collection<Bucket> collection, long resolutionInMs, long flushingPeriodInMs) {
        this.collection = collection;
        this.resolutionInMs = resolutionInMs;
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::flush, flushingPeriodInMs, flushingPeriodInMs, TimeUnit.MILLISECONDS);
    }

    public void ingestPoint(BucketAttributes attributes, long timestamp, long value) {
        long index = timestampToIndex(timestamp);
        Map<BucketAttributes, BucketBuilder> bucketsForTimestamp = seriesQueue.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
        bucketsForTimestamp.computeIfAbsent(attributes, k -> BucketBuilder.create(index).withAttributes(attributes)).ingest(value);
    }

    // TODO flush method can be improved with a batcher (run when a specific timeout ends, or a specific amount of data is collected)
    public void flush() {
        try {
            debug("Flushing");
            long now = System.currentTimeMillis();
            long currentInterval = timestampToIndex(now);
            seriesQueue.forEach((k, v) -> {
                if (k < currentInterval) {
                    // Remove the entry from the map and iterate over it afterwards
                    // This enables concurrent execution of flushing and ingestion
                    Map<BucketAttributes, BucketBuilder> buckets = seriesQueue.remove(k);
                    buckets.forEach((attributes, bucketBuilder) -> collection.save(bucketBuilder.build()));
                }
            });
            flushCount.increment();
            debug("Flushed");
        } catch (Throwable e) {
            logger.error("Error while flushing", e);
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

    private long timestampToIndex(long timestamp) {
        return timestamp - timestamp % resolutionInMs;
    }

    private void debug(String message) {
        if (logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }

    private void trace(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace(message);
        }
    }
}

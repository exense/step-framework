package step.core.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TimeSeriesPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesPipeline.class);

    private final Collection<Bucket> bucketCollection;

    public TimeSeriesPipeline(Collection<Bucket> bucketCollection) {
        this.bucketCollection = bucketCollection;
    }

    public IngestionPipeline newIngestionPipeline() {
        return new IngestionPipeline();
    }

    public IngestionPipeline newAutoFlushingIngestionPipeline(long flushingPeriodMs) {
        return new IngestionPipeline(flushingPeriodMs);
    }

    public Map<Long, Bucket> query(Map<String, String> criteria, long from, long to, long resolutionMs) {
        Map<Long, BucketBuilder> result = new ConcurrentHashMap<>();
        long t1 = System.currentTimeMillis();
        Filter query = buildQuery(criteria, from, to);
        LongAdder bucketCount = new LongAdder();
        bucketCollection.find(query, null, null, null, 0).forEach(bucket -> {
            bucketCount.increment();
            // This implementation uses the start time of the bucket as index
            long begin = bucket.getBegin();
            long index = begin - begin % resolutionMs;
            result.computeIfAbsent(index, k -> BucketBuilder.create(index)).accumulate(bucket);
        });
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    private Filter buildQuery(Map<String, String> criteria, long from, long to) {
        ArrayList<Filter> filters = new ArrayList<>();
        filters.add(Filters.gte("begin", from));
        filters.add(Filters.lte("begin", to));
        filters.addAll(criteria.entrySet().stream()
                .map(e -> Filters.equals("attributes." + e.getKey(), e.getValue())).collect(Collectors.toList()));
        return Filters.and(filters);
    }

    public class IngestionPipeline implements AutoCloseable {

        private final Map<Map<String, String>, Map<Long, BucketBuilder>> series = new ConcurrentHashMap<>();
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final ScheduledExecutorService scheduler;
        // TODO make this configurable
        private final long resolutionMs = 1000L;
        private final LongAdder flushCount = new LongAdder();

        public IngestionPipeline() {
            scheduler = null;
        }

        public IngestionPipeline(long periodInMs) {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(this::flush, periodInMs, periodInMs, TimeUnit.MILLISECONDS);
        }

        public void ingestPoint(Map<String, String> attributes, long timestamp, long value) {
            trace("Acquiring read lock to ingest point");
            lock.readLock().lock();
            try {
                trace("Lock acquired to ingest point");
                Map<Long, BucketBuilder> buckets = series.computeIfAbsent(attributes, k -> new ConcurrentHashMap<>());
                long index = timestamp - timestamp % resolutionMs;
                BucketBuilder bucketBuilder = buckets.computeIfAbsent(index, k ->
                        BucketBuilder.create(index).withAttributes(attributes));
                bucketBuilder.ingest(value);
            } finally {
                lock.readLock().unlock();
            }
            trace("Lock released");
        }

        public void flush() {
            debug("Trying to acquire write lock");
            lock.writeLock().lock();
            try {
                debug("Got write lock");
                // Persist each bucket
                series.forEach((k, v) -> v.forEach((index, bucketBuilder) -> {
                    bucketCollection.save(bucketBuilder.build());
                }));
                series.clear();
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

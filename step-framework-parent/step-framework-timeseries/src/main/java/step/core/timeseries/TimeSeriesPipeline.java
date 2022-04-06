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

    public IngestionPipeline newIngestionPipeline(long resolutionMs) {
        return new IngestionPipeline(resolutionMs);
    }

    public IngestionPipeline newAutoFlushingIngestionPipeline(long resolutionMs, long flushingPeriodMs) {
        return new IngestionPipeline(resolutionMs, flushingPeriodMs);
    }

    /**
     * @return a new query builder instance
     */
    public Query query() {
        return new Query(this);
    }

    public Map<Map<String, String>, Map<Long, Bucket>> runQuery(Query query) {
        Map<Map<String, String>, Map<Long, BucketBuilder>> resultBuilder = new ConcurrentHashMap<>();
        long t1 = System.currentTimeMillis();
        Filter filter = buildFilter(query);
        LongAdder bucketCount = new LongAdder();
        bucketCollection.find(filter, null, null, null, 0).forEach(bucket -> {
            bucketCount.increment();
            // This implementation uses the start time of the bucket as index
            long begin = bucket.getBegin();
            long index = begin - begin % query.intervalSizeMs;

            Map<String, String> bucketLabels = bucket.getAttributes();
            Map<String, String> seriesKey;
            if (query.groupDimensions != null) {
                seriesKey = bucketLabels.entrySet().stream().filter(e ->
                        query.groupDimensions.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else {
                seriesKey = Map.of();
            }
            Map<Long, BucketBuilder> series = resultBuilder.computeIfAbsent(seriesKey, k -> new ConcurrentHashMap<>());
            series.computeIfAbsent(index, k -> BucketBuilder.create(index)).accumulate(bucket);
        });
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }

        return resultBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, builder -> builder.getValue().build()))));
    }

    private Filter buildFilter(Query query) {
        ArrayList<Filter> filters = new ArrayList<>();
        if (query.from != null) {
            filters.add(Filters.gte("begin", query.from));
        }
        if (query.to != null) {
            filters.add(Filters.lte("begin", query.to));
        }
        if (query.filters != null) {
            filters.addAll(query.filters.entrySet().stream()
                    .map(e -> Filters.equals("attributes." + e.getKey(), e.getValue())).collect(Collectors.toList()));
        }
        return Filters.and(filters);
    }

    public class IngestionPipeline implements AutoCloseable {

        private final Map<Map<String, String>, Map<Long, BucketBuilder>> series = new ConcurrentHashMap<>();
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final ScheduledExecutorService scheduler;
        private final long resolutionMs;
        private final LongAdder flushCount = new LongAdder();

        public IngestionPipeline(long resolutionMs) {
            this.resolutionMs = resolutionMs;
            scheduler = null;
        }

        public IngestionPipeline(long resolutionMs, long flushingPeriodInMs) {
            this.resolutionMs = resolutionMs;
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(this::flush, flushingPeriodInMs, flushingPeriodInMs, TimeUnit.MILLISECONDS);
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
                series.forEach((k, v) -> v.forEach((index, bucketBuilder) ->
                        bucketCollection.save(bucketBuilder.build())
                ));
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

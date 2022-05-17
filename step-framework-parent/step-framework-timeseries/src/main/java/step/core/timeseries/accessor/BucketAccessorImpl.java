package step.core.timeseries.accessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.accessors.AbstractAccessor;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.timeseries.Bucket;
import step.core.timeseries.BucketBuilder;
import step.core.timeseries.Query;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class BucketAccessorImpl extends AbstractAccessor<Bucket> implements BucketAccessor {

    private static final Logger logger = LoggerFactory.getLogger(BucketAccessorImpl.class);

    public BucketAccessorImpl(Collection<Bucket> collectionDriver) {
        super(collectionDriver);
    }

    private Filter buildFilter(Query query) {
        ArrayList<Filter> filters = new ArrayList<>();
        if (query.getFrom() != null) {
            filters.add(Filters.gte("begin", query.getFrom()));
        }
        if (query.getTo() != null) {
            filters.add(Filters.lte("begin", query.getTo()));
        }
        if (query.getFilters() != null) {
            filters.addAll(query.getFilters().entrySet().stream()
                    .map(e -> Filters.equals("attributes." + e.getKey(), e.getValue())).collect(Collectors.toList()));
        }
        return Filters.and(filters);
    }

    @Override
    public Map<Map<String, Object>, Map<Long, Bucket>> collectBuckets(Query query) {
        Map<Map<String, Object>, Map<Long, BucketBuilder>> resultBuilder = new ConcurrentHashMap<>();
        long t1 = System.currentTimeMillis();
        Filter filter = buildFilter(query);
        LongAdder bucketCount = new LongAdder();
        collectionDriver.find(filter, null, null, null, 0).forEach(bucket -> {
            bucketCount.increment();
            // This implementation uses the start time of the bucket as index
            long begin = bucket.getBegin();
            long index = begin - begin % query.getIntervalSizeMs();

            Map<String, Object> bucketLabels = bucket.getAttributes();
            Map<String, Object> seriesKey;
            if (query.getGroupDimensions() != null) {
                seriesKey = bucketLabels.entrySet().stream().filter(e ->
                        query.getGroupDimensions().contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else {
                seriesKey = Map.of();
            }
            Map<Long, BucketBuilder> series = resultBuilder.computeIfAbsent(seriesKey, k -> new TreeMap<>());
            series.computeIfAbsent(index, k -> BucketBuilder.create(index)).accumulate(bucket);
        });
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }
        // TODO here we should merge bucket with bucketBuilder to avoid this complete iteration
        return resultBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, builder -> builder.getValue().build()))));
    }
}

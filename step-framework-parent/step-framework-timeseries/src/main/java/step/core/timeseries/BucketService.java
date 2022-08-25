package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class BucketService {

    private static final Logger logger = LoggerFactory.getLogger(BucketService.class);

    private final int resolution;

    private final Collection<Bucket> collectionDriver;

    public BucketService(CollectionFactory collectionFactory, int resolution) {
        this.collectionDriver = collectionFactory.getCollection(BucketAccessor.ENTITY_NAME, Bucket.class);
        this.resolution = resolution;
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

    public TimeSeriesChartResponse collect(Query query) {
        List<Bucket[]> bucketsMatrix = new ArrayList<>();
        List<BucketAttributes> matrixLegend = new ArrayList<>() {};
        Filter filter = buildFilter(query);
        long start = query.getFrom() - query.getFrom() % this.resolution;
        long end = query.getTo() + (this.resolution - query.getTo() % this.resolution);
        int timeIntervals = (int) Math.ceil((double) (end - start) / query.getIntervalSizeMs());
        // object - index in the matrixKeys
        Map<BucketAttributes, Integer> foundSeriesKeys = new HashMap<>();
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        collectionDriver.find(filter, null, null, null, 0).forEach(bucket -> {
            bucketCount.increment();
            BucketAttributes bucketAttributes = bucket.getAttributes();

            Bucket[] series = null;
            BucketAttributes seriesKey;
            if (CollectionUtils.isNotEmpty(query.getGroupDimensions())) {
                seriesKey = bucketAttributes.subset(query.getGroupDimensions());
            } else {
                seriesKey = new BucketAttributes();
            }
            if (foundSeriesKeys.containsKey(seriesKey)) {
                series = bucketsMatrix.get(foundSeriesKeys.get(seriesKey));
            } else { // we have a new series
                int seriesCount = bucketsMatrix.size();
                series = new Bucket[timeIntervals];
                bucketsMatrix.add(series);
                foundSeriesKeys.put(seriesKey, seriesCount);
                matrixLegend.add(seriesKey);
            }
            long begin = bucket.getBegin();
            int beginAnchor = (int) ((begin - start) / query.getIntervalSizeMs());
//            int bucketIndex = (int) ((beginAnchor - start) / query.getIntervalSizeMs());
            Bucket existingBucket = series[beginAnchor];
            if (existingBucket == null) {
                existingBucket = bucket;
            } else {
                existingBucket = new BucketBuilder(existingBucket).accumulate(bucket).build();
            }
            series[beginAnchor] = existingBucket;
        });
        long t2 = System.currentTimeMillis();
//        if (logger.isDebugEnabled()) {
//            logger.info("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
//        }

        return new TimeSeriesChartResponse(start, end, query.getIntervalSizeMs(), bucketsMatrix, matrixLegend);
    }

    // this is the old implementation
//    public Map<Map<String, Object>, Map<Long, Bucket>> collectBuckets(Query query) {
//        Map<Map<String, Object>, Map<Long, BucketBuilder>> resultBuilder = new ConcurrentHashMap<>();
//        long t1 = System.currentTimeMillis();
//        Filter filter = buildFilter(query);
//        LongAdder bucketCount = new LongAdder();
//        collectionDriver.find(filter, null, null, null, 0).forEach(bucket -> {
//            bucketCount.increment();
//            // This implementation uses the start time of the bucket as index
//            long begin = bucket.getBegin();
//            long index = begin - begin % query.getIntervalSizeMs();
//
//            Map<String, Object> bucketLabels = bucket.getAttributes();
//            Map<String, Object> seriesKey;
//            if (query.getGroupDimensions() != null) {
//                seriesKey = bucketLabels.entrySet().stream().filter(e ->
//                        query.getGroupDimensions().contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//            } else {
//                seriesKey = Map.of();
//            }
//            Map<Long, BucketBuilder> series = resultBuilder.computeIfAbsent(seriesKey, k -> new TreeMap<>());
//            series.computeIfAbsent(index, k -> BucketBuilder.create(index)).accumulate(bucket);
//        });
//        long t2 = System.currentTimeMillis();
//        if (logger.isDebugEnabled()) {
//            logger.debug("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
//        }
//        // TODO here we should merge bucket with bucketBuilder to avoid this complete iteration
//        return resultBuilder.entrySet().stream().collect(
//                Collectors.toMap(
//                        Map.Entry::getKey,
//                        pair -> pair.getValue().entrySet().stream().collect(
//                                Collectors.toMap(Map.Entry::getKey, builder -> builder.getValue().build(),
//                                        (o1, o2) -> o1, TreeMap::new)
//                        )
//                )
//        );
//    }

}

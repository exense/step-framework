package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class TimeSeriesAggregationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesAggregationPipeline.class);

    private final int resolution;

    private final Collection<Bucket> collection;

    protected TimeSeriesAggregationPipeline(Collection<Bucket> collectionDriver, int resolution) {
        this.collection = collectionDriver;
        this.resolution = resolution;
    }

    private Filter buildFilter(TimeSeriesAggregationQuery query) {
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

    public TimeSeriesAggregationQuery newQuery() {
        return new TimeSeriesAggregationQuery(this);
    }

    protected TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        List<Bucket[]> bucketsMatrix = new ArrayList<>();
        List<BucketAttributes> matrixLegend = new ArrayList<>();
        Filter filter = buildFilter(query);
        long start = query.getFrom() - query.getFrom() % resolution;
        long end = query.getTo() + (resolution - query.getTo() % resolution);
        int timeIntervals = (int) Math.ceil((double) (end - start) / query.getIntervalSizeMs());
        // object - index in the matrixKeys
        Map<BucketAttributes, Integer> foundSeriesKeys = new HashMap<>();
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        collection.find(filter, null, null, null, 0).forEach(bucket -> {
            bucketCount.increment();
            BucketAttributes bucketAttributes = bucket.getAttributes();

            Bucket[] series;
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

        return new TimeSeriesAggregationResponse(start, end, query.getIntervalSizeMs(), bucketsMatrix, matrixLegend);
    }
}

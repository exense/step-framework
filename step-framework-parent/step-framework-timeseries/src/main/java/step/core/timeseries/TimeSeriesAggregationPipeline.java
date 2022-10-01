package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TimeSeriesAggregationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesAggregationPipeline.class);

    private final long resolution;
    private final Collection<Bucket> collection;
    private final Function<Long, Function<Long, Long>> indexProjectionFunctionFactory;

    protected TimeSeriesAggregationPipeline(Collection<Bucket> collectionDriver, long resolution, Function<Long, Function<Long, Long>> indexProjectionFunctionFactory) {
        this.collection = collectionDriver;
        this.resolution = resolution;
        this.indexProjectionFunctionFactory = indexProjectionFunctionFactory;
    }

    protected long getResolution() {
        return resolution;
    }

    protected Function<Long, Function<Long, Long>> getIndexProjectionFunctionFactory() {
        return indexProjectionFunctionFactory;
    }

    private Filter buildFilter(TimeSeriesAggregationQuery query) {
        ArrayList<Filter> filters = new ArrayList<>();
        if (query.getBucketIndexFrom() != null) {
            filters.add(Filters.gte("begin", query.getBucketIndexFrom()));
        }
        if (query.getBucketIndexTo() != null) {
            filters.add(Filters.lte("begin", query.getBucketIndexTo()));
        }

        if (query.getFilters() != null) {
            filters.addAll(query.getFilters().entrySet().stream()
                    .map(e -> Filters.equals("attributes." + e.getKey(), e.getValue())).collect(Collectors.toList()));
        }
        return Filters.and(filters);
    }

    public TimeSeriesAggregationQuery newQuery() {
        return new TimeSeriesAggregationQuery(this, indexProjectionFunctionFactory.apply(resolution));
    }

    protected TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        Map<BucketAttributes, Map<Long, BucketBuilder>> seriesBuilder = new HashMap<>();

        Function<Long, Long> indexProjectionFunction = query.getIndexProjectionFunction();
        Filter filter = buildFilter(query);
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        collection.find(filter, null, null, null, 0).forEach(bucket -> {
            bucketCount.increment();
            BucketAttributes bucketAttributes = bucket.getAttributes();

            BucketAttributes seriesKey;
            if (CollectionUtils.isNotEmpty(query.getGroupDimensions())) {
                seriesKey = bucketAttributes.subset(query.getGroupDimensions());
            } else {
                seriesKey = new BucketAttributes();
            }
            Map<Long, BucketBuilder> series = seriesBuilder.computeIfAbsent(seriesKey, a -> new TreeMap<>());

            long begin = bucket.getBegin();
            long index = indexProjectionFunction.apply(begin);
            series.computeIfAbsent(index, BucketBuilder::new).accumulate(bucket);
        });
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.info("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }

        Map<BucketAttributes, Map<Long, Bucket>> result = seriesBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().build()))));
        return new TimeSeriesAggregationResponse(result).withAxis(query.drawAxis());
    }
}

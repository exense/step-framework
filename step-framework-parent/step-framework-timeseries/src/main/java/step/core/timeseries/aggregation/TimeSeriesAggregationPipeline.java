package step.core.timeseries.aggregation;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;
import step.core.timeseries.TimeSeriesFilterBuilder;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;


public class TimeSeriesAggregationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesAggregationPipeline.class);

    private final long sourceResolution;
    private final Collection<Bucket> collection;

    public TimeSeriesAggregationPipeline(Collection<Bucket> collectionDriver, long resolution) {
        this.collection = collectionDriver;
        this.sourceResolution = resolution;
    }

    protected long getSourceResolution() {
        return sourceResolution;
    }

    public TimeSeriesAggregationQueryBuilder newQueryBuilder() {
        return new TimeSeriesAggregationQueryBuilder(this);
    }

    protected TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        Map<BucketAttributes, Map<Long, BucketBuilder>> seriesBuilder = new HashMap<>();

        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        Function<Long, Long> projectionFunction = query.getProjectionFunction();
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

            long index = projectionFunction.apply(bucket.getBegin());
            series.computeIfAbsent(index, i -> new BucketBuilder(i, i + query.getBucketSize())).accumulate(bucket);
        });
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.info("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }

        Map<BucketAttributes, Map<Long, Bucket>> result = seriesBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().build()))));
        return new TimeSeriesAggregationResponse(result, query.getBucketSize()).withAxis(query.drawAxis());
    }
}
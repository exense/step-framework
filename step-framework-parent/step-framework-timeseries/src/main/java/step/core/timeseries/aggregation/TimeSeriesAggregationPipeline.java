package step.core.timeseries.aggregation;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.timeseries.TimeSeriesCollection;
import step.core.timeseries.TimeSeriesFilterBuilder;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TimeSeriesAggregationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesAggregationPipeline.class);
    private Map<Long, TimeSeriesCollection> collectionsByResolution = new TreeMap<>();
    // resolution - array index
    private Map<Long, Integer> resolutionsIndexes = new HashMap<>();
    // sorted
    private final List<TimeSeriesCollection> collections;

    public TimeSeriesAggregationPipeline(List<TimeSeriesCollection> collections) {
        this.collections = collections;
        for (int i = 0; i < collections.size(); i++) {
            TimeSeriesCollection collection = collections.get(i);
            collectionsByResolution.put(collection.getResolution(), collection);
            resolutionsIndexes.put(collection.getResolution(), i);
        }
    }
    
    private long roundRequiredResolution(long targetResolution) {
        List<Long> availableResolutions = getAvailableResolutions();
        for (int i = 1; i < availableResolutions.size(); i++) {
            if (availableResolutions.get(i) > targetResolution) {
                return availableResolutions.get(i - 1);
            }
        }
        return availableResolutions.get(availableResolutions.size() - 1); // return last resolution 
    }
    
    private TimeSeriesProcessedParams processQueryParams(TimeSeriesAggregationQuery query, long sourceResolution) {
        if (query.getBucketsCount() != null && (query.getFrom() == null || query.getTo() == null)) {
            throw new IllegalArgumentException("While splitting, from and to params must be set");
        }
        if (query.getFrom() == null || query.getTo() == null) {
            throw new IllegalArgumentException("From and To parameters must be specified");
        }

        long resultResolution = sourceResolution;
        Long resultFrom = roundDownToMultiple(query.getFrom(), sourceResolution);
        Long resultTo = roundUpToMultiple(query.getTo(), sourceResolution);
        if (query.isShrink()) { // we expand the interval to the closest completed resolutions
            resultResolution = resultTo - resultFrom;
        } else {
            Integer bucketsCount = query.getBucketsCount();
            if (bucketsCount != null && bucketsCount > 0) {
                if ((resultTo - resultFrom) / sourceResolution <= bucketsCount) { // not enough buckets
                    resultResolution = sourceResolution;
                } else {
                    long difference = resultTo - resultFrom;
                    resultResolution = Math.round(difference / (double) bucketsCount);
                    // there are situation when resultResolution/sourceResolution is below 0.5, and that would end up rounded in 0.
                    resultResolution = Math.max(Math.round((double) resultResolution / sourceResolution), 1) * sourceResolution; // round to nearest multiple, up or down
                }
            } else {
                Long proposedResolution = query.getBucketsResolution();
                if (proposedResolution != null && proposedResolution != 0) {
                    resultResolution = Math.max(sourceResolution, roundDownToMultiple(proposedResolution, sourceResolution));
                    resultResolution = roundDownToMultiple(resultResolution, sourceResolution);
                    long diff = resultTo - resultFrom;
                    diff = roundUpToMultiple(diff, resultResolution);
                    resultTo = resultFrom + diff;
                }
            }
        }
        return new TimeSeriesProcessedParams()
                .setFrom(resultFrom)
                .setTo(resultTo)
                .setResolution(resultResolution)
                .setGroupDimensions(query.getGroupDimensions())
                .setFilter(query.getFilter())
                .setShrink(query.isShrink())
                .setCollectAttributeKeys(query.getCollectAttributeKeys())
                .setCollectAttributesValuesLimit(query.getCollectAttributesValuesLimit());
    }
    
    private static long roundUpToMultiple(long value, long multiple) {
        return (long) Math.ceil((double) value / multiple) * multiple;
    }

    private static long roundDownToMultiple(long value, long multiple) {
        return value - value % multiple;
    }
    
    private TimeSeriesCollection chooseTargetCollection(TimeSeriesAggregationQuery query) {
        long idealResolution = getIdealResolution(query);
        long targetResolution = this.roundRequiredResolution(idealResolution); // find an existing resolution close to it
        int targetResolutionIndex = this.resolutionsIndexes.get(targetResolution);
        for (int i = targetResolutionIndex; i > 0; i--) { // find the best resolution with valid TTL
            TimeSeriesCollection targetCollection = this.collections.get(targetResolutionIndex);
            if (collectionTtlCoverInterval(targetCollection, query.getFrom(), query.getTo())) {
                return targetCollection;
            }
        }
        return this.collections.get(0);
    }

    private static long getIdealResolution(TimeSeriesAggregationQuery query) {
        Integer bucketsCount = query.getBucketsCount();
        Long bucketsResolution = query.getBucketsResolution();
        long requestedRange = query.getTo() - query.getFrom();
        int idealBucketsCount = 100;
        long idealResolution;
        if (query.isShrink()) {
            idealResolution = requestedRange / idealBucketsCount;
        } else if (bucketsCount != null && bucketsCount > 0) {
            idealResolution = requestedRange / bucketsCount;
        } else if (bucketsResolution != null && bucketsResolution > 0) {
            idealResolution = bucketsResolution;
        } else {
            idealResolution = requestedRange / idealBucketsCount;
        }
        return idealResolution;
    }

    private boolean collectionTtlCoverInterval(TimeSeriesCollection collection, long from, long to) {
        long ttl = collection.getTtl();
        if (ttl == 0) {
            // housekeeping is disabled
            return true;
        }
        long collectionEnd = System.currentTimeMillis();
        long collectionStart = collectionEnd - ttl;
        return collectionStart <= from && collectionEnd >= to;
    }

    public TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery x) {
        TimeSeriesCollection targetCollection = chooseTargetCollection(x);
        Collection<Bucket> selectedCollection = targetCollection.getCollection();
        long sourceResolution = targetCollection.getResolution();
        TimeSeriesProcessedParams finalParams = processQueryParams(x, sourceResolution);

        Map<BucketAttributes, Map<Long, BucketBuilder>> seriesBuilder = new HashMap<>();

        Filter filter = TimeSeriesFilterBuilder.buildFilter(finalParams);
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = selectedCollection.findLazy(filter, null, null, null, 0)) {
            stream.forEach(bucket -> {
                bucketCount.increment();
                BucketAttributes bucketAttributes = bucket.getAttributes();

                BucketAttributes seriesKey;
                if (CollectionUtils.isNotEmpty(finalParams.getGroupDimensions())) {
                    seriesKey = bucketAttributes.subset(finalParams.getGroupDimensions());
                } else {
                    seriesKey = new BucketAttributes();
                }
                Map<Long, BucketBuilder> series = seriesBuilder.computeIfAbsent(seriesKey, a -> new TreeMap<>());

                long index = calculateBucketBeginAnchor(bucket.getBegin(), finalParams);
                series.computeIfAbsent(index, i -> new BucketBuilder(i, i + getBucketSize(finalParams.getFrom(), finalParams.getTo(), finalParams.isShrink(), sourceResolution))
                        .withAccumulateAttributes(x.getCollectAttributeKeys(), x.getCollectAttributesValuesLimit())).accumulate(bucket);
            });
        }
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.info("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }

        Map<BucketAttributes, Map<Long, Bucket>> result = seriesBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().build()))));
        return new TimeSeriesAggregationResponse(result, finalParams.getResolution())
                .withAxis(drawAxis(finalParams));
    }
    
    public List<Long> getAvailableResolutions() {
		return new ArrayList<>(collectionsByResolution.keySet());
	}
    
    public List<Long> drawAxis(TimeSeriesProcessedParams params) {
        long from = params.getFrom();
        long to = params.getTo();
        ArrayList<Long> legend = new ArrayList<>();
        if (from >= 0 && to > 0) {
            if (params.isShrink()) {
                legend.add(from);
            } else {
                for (long index = from; index < to; index += params.getResolution()) {
                    legend.add(index);
                }
            }
        }
        return legend;
    }

    private long calculateBucketBeginAnchor(long bucketBegin, TimeSeriesProcessedParams params) {
        long rangeFrom = params.getFrom();
        if (params.isShrink()) {
            return rangeFrom;
        } else {
            long distanceFromStart = bucketBegin - rangeFrom;
            return distanceFromStart - distanceFromStart % params.getResolution() + rangeFrom;
        }
    }

    public long getBucketSize(Long from, long to, boolean shrink, long resultResolution) {
        if (shrink) {
            if (from != null) {
                return to - from;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return resultResolution;
        }
    }
}

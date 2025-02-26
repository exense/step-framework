package step.core.timeseries.aggregation;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.timeseries.TimeSeriesCollection;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static step.core.collections.Filters.collectFilterAttributesRecursively;


public class TimeSeriesAggregationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesAggregationPipeline.class);
    // resolution - array index
    private final Map<Long, Integer> resolutionsIndexes = new HashMap<>();
    // sorted
    private final List<TimeSeriesCollection> collections;
    private final int responseMaxIntervals;
    private final int idealResponseIntervals;
    private boolean ttlEnabled;

    public TimeSeriesAggregationPipeline(List<TimeSeriesCollection> collections, int responseMaxIntervals, int idealResponseIntervals, boolean ttlEnabled) {
        this.ttlEnabled = ttlEnabled;
        if (responseMaxIntervals <= 0) {
            throw new IllegalArgumentException("responseMaxIntervals must be greater than 0");
        }
        if (idealResponseIntervals <=0) {
            throw new IllegalArgumentException("idealResponseIntervals must be greater than 0");
        }
        this.responseMaxIntervals = responseMaxIntervals;
        this.idealResponseIntervals = idealResponseIntervals;
        this.collections = collections;
        for (int i = 0; i < collections.size(); i++) {
            TimeSeriesCollection collection = collections.get(i);
            resolutionsIndexes.put(collection.getResolution(), i);
        }
    }

    public void setTtlEnabled(boolean ttlEnabled) {
        this.ttlEnabled = ttlEnabled;
    }

    private Set<String> collectAllUsedAttributes(TimeSeriesAggregationQuery query) {
        Set<String> attributes = new HashSet<>();
        attributes.addAll(query.getGroupDimensions());
        collectFilterAttributesRecursively(query.getFilter(), attributes);

        return attributes;
    }

    /**
     * Process order for calculating the ideal resolution:
     * 1. Split range and round to a good resolution
     * 2. Go from bottom to top and find the lowest resolution with a valid TTL
     * 3. Go backward from the resolution obtained above and choose the first collection which handle all the attributes
     */
    public TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        validateQuery(query);
        Set<String> usedAttributes = collectAllUsedAttributes(query).stream().map(a -> a.replace("attributes.", "")).collect(Collectors.toSet());
        long queryFrom = query.getFrom() != null ? query.getFrom() : 0;
        long queryTo = query.getTo() != null ? query.getTo() : System.currentTimeMillis();
        long idealResolution = 0;
        if (query.getOptimizationType() == TimeSeriesOptimizationType.MOST_ACCURATE) {
            idealResolution = collections.get(0).getResolution(); // first collection with the best resolution
        } else { // most efficient
            idealResolution = this.roundDownToAvailableResolution(getIdealResolution(query));
        }
        TimeSeriesCollection idealAvailableCollection = ttlEnabled ? chooseFirstAvailableCollectionBasedOnTTL(idealResolution, query) : this.collections.get(this.resolutionsIndexes.get(idealResolution));
        idealAvailableCollection = chooseLastCollectionWhichHandleAttributes(idealAvailableCollection.getResolution(), usedAttributes);

        boolean fallbackToHigherResolutionWithValidTTL = idealResolution < idealAvailableCollection.getResolution();
        boolean ttlCovered = ttlEnabled ? collectionTtlCoverInterval(idealAvailableCollection, queryFrom, queryTo) : true;

        long sourceResolution = idealAvailableCollection.getResolution();
        TimeSeriesProcessedParams finalParams = processQueryParams(query, sourceResolution);

        Map<BucketAttributes, Map<Long, BucketBuilder>> seriesBuilder = new HashMap<>();

        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = idealAvailableCollection.queryTimeSeries(finalParams)) {
            stream.forEach(bucket -> {
                bucketCount.increment();
                BucketAttributes bucketAttributes = bucket.getAttributes() != null ? bucket.getAttributes() : new BucketAttributes();

                BucketAttributes seriesKey;
                if (CollectionUtils.isNotEmpty(finalParams.getGroupDimensions())) {
                    seriesKey = bucketAttributes.subset(finalParams.getGroupDimensions());
                } else {
                    seriesKey = new BucketAttributes();
                }
                Map<Long, BucketBuilder> series = seriesBuilder.computeIfAbsent(seriesKey, a -> new TreeMap<>());

                long index = calculateBucketBeginAnchor(bucket.getBegin(), finalParams);
                series.computeIfAbsent(index, i -> new BucketBuilder(i, i + getBucketSize(finalParams.getFrom(), finalParams.getTo(), finalParams.isShrink(), finalParams.getResolution()))
                        .withAccumulateAttributes(query.getCollectAttributeKeys(), query.getCollectAttributesValuesLimit())).accumulate(bucket);
            });
        }
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }

        Map<BucketAttributes, Map<Long, Bucket>> result = seriesBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().build()))));

        return new TimeSeriesAggregationResponseBuilder()
                .setSeries(result)
                .setStart(finalParams.getFrom())
                .setEnd(finalParams.getTo())
                .setResolution(finalParams.getResolution())
                .setCollectionResolution(idealAvailableCollection.getResolution())
                .setHigherResolutionUsed(fallbackToHigherResolutionWithValidTTL)
                .setTtlCovered(ttlCovered)
                .build();
    }
    
    private long roundDownToAvailableResolution(long targetResolution) {
        List<Long> availableResolutions = getAvailableResolutions();
        for (int i = 1; i < availableResolutions.size(); i++) {
            if (availableResolutions.get(i) > targetResolution) {
                return availableResolutions.get(i - 1);
            }
        }
        return availableResolutions.get(availableResolutions.size() - 1); // return last resolution 
    }
    
    private TimeSeriesProcessedParams processQueryParams(TimeSeriesAggregationQuery query, long sourceResolution) {
        if (query.getFrom() == null) {
            throw new IllegalArgumentException("From parameters must be specified");
        }
        // if 'to' parameter is not specified, we take the current time.
        long toParameter = query.getTo() != null ? query.getTo() : System.currentTimeMillis();
        long resultResolution = sourceResolution;

        long resultFrom = roundDownToMultiple(query.getFrom(), sourceResolution);
        long resultTo = roundUpToMultiple(toParameter, sourceResolution);
        long rangeDiff = resultTo - resultFrom;

        if (query.isShrink()) { // we expand the interval to the closest completed resolutions
            resultResolution = rangeDiff;
        } else {
            Integer bucketsCount = query.getBucketsCount();
            if (bucketsCount != null && bucketsCount > 0) {
                resultResolution = getResolutionBasedOnBucketsCount(sourceResolution, rangeDiff, bucketsCount);
            } else {
                Long proposedResolution = query.getBucketsResolution();
                if (proposedResolution != null && proposedResolution != 0) {
                    resultResolution = Math.max(sourceResolution, roundDownToMultiple(proposedResolution, sourceResolution));
                    resultResolution = roundDownToMultiple(resultResolution, sourceResolution);
                    rangeDiff = roundUpToMultiple(rangeDiff, resultResolution);
                    resultTo = resultFrom + rangeDiff;
                } else { // no resolution settings specified
                    resultResolution = getResolutionBasedOnBucketsCount(sourceResolution, rangeDiff, idealResponseIntervals);
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

    private static long getResolutionBasedOnBucketsCount(long sourceResolution, long rangeDiff, Integer bucketsCount) {
        long resultResolution;
        if (rangeDiff / sourceResolution <= bucketsCount) { // not enough buckets
            resultResolution = sourceResolution;
        } else {
            resultResolution = Math.round(rangeDiff / (double) bucketsCount);
            // there are situation when resultResolution/sourceResolution is below 0.5, and that would end up rounded in 0.
            resultResolution = Math.max(Math.round((double) resultResolution / sourceResolution), 1) * sourceResolution; // round to nearest multiple, up or down
        }
        return resultResolution;
    }

    private static long roundUpToMultiple(long value, long multiple) {
        return (long) Math.ceil((double) value / multiple) * multiple;
    }

    private static long roundDownToMultiple(long value, long multiple) {
        return value - value % multiple;
    }

    private void validateQuery(TimeSeriesAggregationQuery query) {
        if (query.getBucketsCount() != null){
            if (query.getFrom() == null || query.getTo() == null) {
                throw new IllegalArgumentException("While splitting, from and to params must be set");
            }
            if (responseMaxIntervals > 0 && query.getBucketsCount() > responseMaxIntervals) {
                throw new IllegalArgumentException("Buckets count must be less than or equal to " + responseMaxIntervals);
            }
        }
        if (query.getBucketsResolution() != null) {
            long firstCollectionResolution = collections.get(0).getResolution();
            if (query.getBucketsResolution() < firstCollectionResolution) {
                throw new IllegalArgumentException("Buckets resolution must be less than or equal to the minimum registered collection: " + firstCollectionResolution);
            }
        }
        if (query.getFrom() != null && query.getTo() != null) {
            if (query.getFrom() > query.getTo()) {
                throw new IllegalArgumentException("Invalid requested range: 'from' timestamp is greater than 'to' timestamp.");
            }
            if (responseMaxIntervals > 0 && query.getBucketsResolution() != null && (query.getTo() - query.getFrom()) / query.getBucketsResolution() > responseMaxIntervals) {
                throw new IllegalArgumentException("Requested resolution of " + query.getBucketsResolution() + " ms exceeds the maximum response intervals: " + responseMaxIntervals);
            }
        }

    }
    
    private TimeSeriesCollection chooseFirstAvailableCollectionBasedOnTTL(long resolution, TimeSeriesAggregationQuery query) {
        long from = query.getFrom() != null ? query.getFrom() : 0;
        long to = query.getTo() != null ? query.getTo() : System.currentTimeMillis();
        int targetResolutionIndex = this.resolutionsIndexes.get(resolution);
        for (int i = targetResolutionIndex; i < this.collections.size(); i++) { // find the best resolution with valid TTL
            TimeSeriesCollection targetCollection = this.collections.get(i);
            if (collectionTtlCoverInterval(targetCollection, from, to)) {
                return targetCollection;
            }
        }
        return this.collections.get(this.collections.size() - 1); // return highest resolution
    }

    private TimeSeriesCollection chooseLastCollectionWhichHandleAttributes(long idealResolution, Set<String> queryAttributes) {
        Integer idealResolutionIndex = this.resolutionsIndexes.get(idealResolution);
        if (CollectionUtils.isEmpty(queryAttributes)) {
            return this.collections.get(idealResolutionIndex);
        } else {
            for (int i = idealResolutionIndex; i >= 0; i--) {
                TimeSeriesCollection currentCollection = this.collections.get(i);
                if (CollectionUtils.isEmpty(currentCollection.getIgnoredAttributes()) || currentCollection.getIgnoredAttributes().stream().noneMatch(queryAttributes::contains)) {
                    return currentCollection;
                }
            }
        }
        return this.collections.get(0);
    }

    private long getIdealResolution(TimeSeriesAggregationQuery query) {
        Integer bucketsCount = query.getBucketsCount();
        Long bucketsResolution = query.getBucketsResolution();
        long queryTo = query.getTo() != null ? query.getTo() : System.currentTimeMillis();
        long requestedRange = queryTo - query.getFrom();
        long idealResolution;
        if (query.isShrink()) {
            idealResolution = requestedRange / idealResponseIntervals;
        } else if (bucketsCount != null && bucketsCount > 0) {
            idealResolution = requestedRange / bucketsCount;
        } else if (bucketsResolution != null && bucketsResolution > 0) {
            idealResolution = bucketsResolution;
        } else {
            idealResolution = requestedRange / idealResponseIntervals;
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

    public List<Long> getAvailableResolutions() {
		return this.collections.stream().map(TimeSeriesCollection::getResolution).collect(Collectors.toList());
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

    public int getResponseMaxIntervals() {
        return responseMaxIntervals;
    }
}

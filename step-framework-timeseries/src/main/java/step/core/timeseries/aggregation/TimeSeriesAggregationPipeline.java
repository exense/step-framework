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
    // resolution - array index
    private final Map<Long, Integer> resolutionsIndexes = new HashMap<>();
    // sorted
    private final List<TimeSeriesCollection> collections;

    private boolean ttlEnabled;

    public TimeSeriesAggregationPipeline(List<TimeSeriesCollection> collections, boolean ttlEnabled) {
        this.collections = collections;
        this.ttlEnabled = ttlEnabled;
        for (int i = 0; i < collections.size(); i++) {
            TimeSeriesCollection collection = collections.get(i);
            resolutionsIndexes.put(collection.getResolution(), i);
        }
    }

    public void setTtlEnabled(boolean ttlEnabled) {
        this.ttlEnabled = ttlEnabled;
    }


    private void collectFilterAttributesRecursively(Filter filter, Set<String> collectedAttributes) {
        if (filter.getField() != null) {
            collectedAttributes.add(filter.getField());
        }
        if (filter.getChildren() != null) {
            filter.getChildren().forEach(c -> collectFilterAttributesRecursively(c, collectedAttributes));
        }
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

        Collection<Bucket> selectedCollection = idealAvailableCollection.getCollection();
        long sourceResolution = idealAvailableCollection.getResolution();
        TimeSeriesProcessedParams finalParams = processQueryParams(query, sourceResolution);

        Map<BucketAttributes, Map<Long, BucketBuilder>> seriesBuilder = new HashMap<>();

        Filter filter = TimeSeriesFilterBuilder.buildFilter(finalParams);

        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = selectedCollection.findLazy(filter, null, null, null, 0)) {
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
        TimeSeriesAggregationResponse response = new TimeSeriesAggregationResponse(result, finalParams.getResolution(), idealAvailableCollection.getResolution(), fallbackToHigherResolutionWithValidTTL, ttlCovered);
        if (query.getFrom() != null && query.getTo() != null) {
            // axis are calculated only when the interval is specified
            response.withAxis(drawAxis(finalParams));
        }

        return response;
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
        if (query.getBucketsCount() != null && (query.getFrom() == null || query.getTo() == null)) {
            throw new IllegalArgumentException("While splitting, from and to params must be set");
        }
        if (query.getFrom() == null) {
            throw new IllegalArgumentException("From parameters must be specified");
        }

        long resultResolution = sourceResolution;
        Long resultFrom = roundDownToMultiple(query.getFrom(), sourceResolution);
        // if 'to' parameter is not specified, we take the current time.
        long toParameter = query.getTo() != null ? query.getTo() : System.currentTimeMillis();
        Long resultTo = roundUpToMultiple(toParameter, sourceResolution);
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

    private void validateQuery(TimeSeriesAggregationQuery query) {
        if (query.getBucketsResolution() != null && query.getBucketsResolution() < collections.get(0).getResolution()) {
            throw new IllegalArgumentException("Buckets resolution must be less than or equal to the minimum registered collection");
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

    private static long getIdealResolution(TimeSeriesAggregationQuery query) {
        Integer bucketsCount = query.getBucketsCount();
        Long bucketsResolution = query.getBucketsResolution();
        long queryTo = query.getTo() != null ? query.getTo() : System.currentTimeMillis();
        long requestedRange = queryTo - query.getFrom();
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


    public List<Long> getAvailableResolutions() {
		return this.collections.stream().map(TimeSeriesCollection::getResolution).collect(Collectors.toList());
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

package step.core.timeseries.aggregation;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.timeseries.TimeSeriesCollection;
import step.core.timeseries.TimeSeriesUtils;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
        if (idealResponseIntervals <= 0) {
            throw new IllegalArgumentException("idealResponseIntervals must be greater than 0");
        }
        this.responseMaxIntervals = responseMaxIntervals;
        this.idealResponseIntervals = idealResponseIntervals;
        this.collections = collections;
        for (int i = 0; i < collections.size(); i++) {
            TimeSeriesCollection collection = collections.get(i);
            resolutionsIndexes.put(collection.getResolutionMs(), i);
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
        long idealResolution = 0;
        if (query.getOptimizationType() == TimeSeriesOptimizationType.MOST_ACCURATE) {
            idealResolution = collections.get(0).getResolutionMs(); // first collection with the best resolution
        } else { // most efficient
            idealResolution = this.roundDownToAvailableResolution(getIdealResolution(query));
        }
        TimeSeriesCollection idealAvailableCollection = ttlEnabled ? chooseFirstAvailableCollectionBasedOnTTL(idealResolution, query) : this.collections.get(this.resolutionsIndexes.get(idealResolution));
        idealAvailableCollection = chooseLastCollectionWhichHandleAttributes(idealAvailableCollection.getResolutionMs(), usedAttributes);

        boolean fallbackToHigherResolutionWithValidTTL = idealResolution < idealAvailableCollection.getResolutionMs();
        boolean ttlCovered = ttlEnabled ? collectionTtlCoverInterval(idealAvailableCollection, queryFrom) : true;

        long sourceResolution = idealAvailableCollection.getResolutionMs();
        TimeSeriesProcessedParams finalParams = processQueryParams(query, sourceResolution);

        Map<BucketAttributes, Map<Long, BucketBuilder>> resultBuilder;
        if (query.getTimeAggregation().isMerge() && query.getGroupAggregation().isMerge()) {
            resultBuilder = collectByMerging(query, finalParams, idealAvailableCollection);
        } else {
            resultBuilder = collectByAggregating(query, finalParams, idealAvailableCollection);
        }

        Map<BucketAttributes, Map<Long, Bucket>> result = resultBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
            e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().buildAggregate()))));

        return new TimeSeriesAggregationResponseBuilder()
            .setSeries(result)
            .setStart(finalParams.getFrom())
            .setEnd(finalParams.getTo())
            .setResolution(finalParams.getResolution())
            .setCollectionResolution(idealAvailableCollection.getResolutionMs())
            .setCollectionIgnoredAttributes(idealAvailableCollection.getIgnoredAttributes())
            .setHigherResolutionUsed(fallbackToHigherResolutionWithValidTTL)
            .setTtlCovered(ttlCovered)
            .build();
    }

    /**
     * Collects the source buckets when both axes merge, i.e. when the whole aggregation amounts to one single merge
     * of all the source buckets of a group falling into the same time bucket. A merge being associative and
     * commutative, the source buckets can be merged directly into their resulting bucket, without materializing the
     * series they belong to. The memory footprint is therefore driven by the number of groups and time buckets of the
     * response, and not by the cardinality of the attributes of the source buckets.
     */
    private Map<BucketAttributes, Map<Long, BucketBuilder>> collectByMerging(TimeSeriesAggregationQuery query, TimeSeriesProcessedParams finalParams, TimeSeriesCollection collection) {
        Map<BucketAttributes, Map<Long, BucketBuilder>> resultBuilder = new HashMap<>();
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = collection.queryTimeSeries(finalParams)) {
            stream.forEach(bucket -> {
                bucketCount.increment();
                BucketAttributes groupAttributes = getGroupAttributes(bucket, finalParams.getGroupDimensions());
                long timeSliceIndex = calculateBucketBeginAnchor(bucket.getBegin(), finalParams);

                Map<Long, BucketBuilder> resultSeriesBuilder = resultBuilder.computeIfAbsent(groupAttributes, a -> new TreeMap<>());
                resultSeriesBuilder.computeIfAbsent(timeSliceIndex, i -> newGroupBucketBuilder(query, finalParams, groupAttributes, i))
                    .merge(bucket);
            });
        }
        logAggregationDuration("merge aggregation", t1, bucketCount);
        return resultBuilder;
    }

    /**
     * Collects the source buckets when at least one of the axes reduces its inputs to a scalar. The time-window
     * aggregation has then to be applied per series, before the group-by aggregation, which requires the series of
     * each group to be materialized.
     */
    private Map<BucketAttributes, Map<Long, BucketBuilder>> collectByAggregating(TimeSeriesAggregationQuery query, TimeSeriesProcessedParams finalParams, TimeSeriesCollection collection) {
        // Perform time-window aggregation and partition the time series:
        // Aggregate each source series into the requested aligned time buckets and assign the resulting series to their respective groups (defined by the group dimensions).
        // Do not perform any cross-series aggregation at this stage.
        Map<Long, Map<BucketAttributes, Map<BucketAttributes, BucketBuilder>>> timeBucketedSeriesByGroup = new HashMap<>();
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = collection.queryTimeSeries(finalParams)) {
            stream.forEach(bucket -> {
                bucketCount.increment();
                // The attributes of the source series
                BucketAttributes bucketAttributes = bucket.getAttributes() != null ? bucket.getAttributes() : new BucketAttributes();
                // The subset of attributes corresponding to the requested group dimensions (group by)
                BucketAttributes groupAttributes = getGroupAttributes(bucket, finalParams.getGroupDimensions());
                // The time slice index on the result series
                long timeSliceIndex = calculateBucketBeginAnchor(bucket.getBegin(), finalParams);
                // Get or create the time slice corresponding to the time index of the current bucket on the result series (aligned)
                Map<BucketAttributes, Map<BucketAttributes, BucketBuilder>> timeSlice = timeBucketedSeriesByGroup.computeIfAbsent(timeSliceIndex, a -> new HashMap<>());
                // Get or create the group of builders corresponding to the current group (defined by the group dimensions)
                Map<BucketAttributes, BucketBuilder> indexSeriesBuckets = timeSlice.computeIfAbsent(groupAttributes, a -> new HashMap<>());
                // Get the builder for the attributes of the current bucket. The full attributes of the series are kept
                // at this stage, so that the attribute collection can be performed on them during the group-by aggregation
                BucketBuilder bucketBuilder = indexSeriesBuckets.computeIfAbsent(bucketAttributes, a -> new BucketBuilder(query.getTimeAggregation(), timeSliceIndex, getBucketEnd(timeSliceIndex, finalParams)).withAttributes(bucketAttributes));
                // Merge the current source bucket into the builder. The configured time-window aggregation is
                // applied when the builder is reduced, at the group-by stage
                bucketBuilder.merge(bucket);
            });
        }
        logAggregationDuration("time-window aggregation", t1, bucketCount);

        // Aggregate the grouped series:
        // For each time bucket, apply the configured group-by aggregation across the aligned series in each group.
        Map<BucketAttributes, Map<Long, BucketBuilder>> resultBuilder = new HashMap<>();
        // For each time slice
        timeBucketedSeriesByGroup.forEach((timeSliceIndex, timeSlice) -> {
            // For each group
            timeSlice.forEach((groupAttributes, group) -> {
                // For each series of the group
                group.forEach((seriesAttributes, series) -> {
                    Map<Long, BucketBuilder> resultSeriesBuilder = resultBuilder.computeIfAbsent(groupAttributes, a -> new TreeMap<>());
                    BucketBuilder bucketBuilder = resultSeriesBuilder.computeIfAbsent(timeSliceIndex, i -> newGroupBucketBuilder(query, finalParams, groupAttributes, i));
                    // Aggregate the series into the group. How the series contributes is defined by the
                    // time-window aggregation it was built with
                    bucketBuilder.aggregate(series);
                });
            });
        });
        return resultBuilder;
    }

    private BucketAttributes getGroupAttributes(Bucket bucket, Set<String> groupDimensions) {
        BucketAttributes bucketAttributes = bucket.getAttributes();
        if (bucketAttributes == null || CollectionUtils.isEmpty(groupDimensions)) {
            return new BucketAttributes();
        }
        return bucketAttributes.subset(groupDimensions);
    }

    private BucketBuilder newGroupBucketBuilder(TimeSeriesAggregationQuery query, TimeSeriesProcessedParams finalParams, BucketAttributes groupAttributes, long timeSliceIndex) {
        return new BucketBuilder(query.getGroupAggregation(), timeSliceIndex, getBucketEnd(timeSliceIndex, finalParams))
            // The group attributes are copied, so that collecting the attributes doesn't mutate the key of the response
            .withAttributes(new BucketAttributes(groupAttributes))
            .withAccumulateAttributes(query.getCollectAttributeKeys(), query.getCollectAttributesValuesLimit());
    }

    private void logAggregationDuration(String aggregationName, long startTime, LongAdder bucketCount) {
        if (logger.isDebugEnabled()) {
            logger.debug("Performed " + aggregationName + " in " + (System.currentTimeMillis() - startTime) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }
    }

    private long getBucketEnd(Long i, TimeSeriesProcessedParams finalParams) {
        return i + getBucketSize(finalParams.getFrom(), finalParams.getTo(), finalParams.isShrink(), finalParams.getResolution());
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
        if (query.getBucketsCount() != null) {
            if (query.getFrom() == null || query.getTo() == null) {
                throw new IllegalArgumentException("While splitting, from and to params must be set");
            }
            if (responseMaxIntervals > 0 && query.getBucketsCount() > responseMaxIntervals) {
                throw new IllegalArgumentException("Buckets count must be less than or equal to " + responseMaxIntervals);
            }
        }
        if (query.getBucketsResolution() != null) {
            long firstCollectionResolution = collections.get(0).getResolutionMs();
            if (query.getBucketsResolution() < firstCollectionResolution) {
                throw new IllegalArgumentException("Buckets resolution must be less than or equal to the minimum registered collection: " + firstCollectionResolution);
            }
        }
        if (query.getFrom() != null && query.getTo() != null) {
            if (query.getFrom() > query.getTo()) {
                throw new IllegalArgumentException("Invalid requested range: 'from' timestamp is greater than 'to' timestamp.");
            }
            if (responseMaxIntervals > 0 && query.getBucketsResolution() != null && (query.getTo() - query.getFrom()) / query.getBucketsResolution() > responseMaxIntervals) {
                String formattedResolution = TimeSeriesUtils.formatMilliseconds(query.getBucketsResolution());
                throw new IllegalArgumentException(String.format("The requested time resolution of %s is too small for the selected time range and would exceed the maximum number of buckets (%d). Please choose a higher time resolution or a shorter time range.", formattedResolution, responseMaxIntervals));
            }
        }

    }

    private TimeSeriesCollection chooseFirstAvailableCollectionBasedOnTTL(long resolution, TimeSeriesAggregationQuery query) {
        long from = query.getFrom() != null ? query.getFrom() : 0;
        int targetResolutionIndex = this.resolutionsIndexes.get(resolution);
        for (int i = targetResolutionIndex; i < this.collections.size(); i++) { // find the best resolution with valid TTL
            TimeSeriesCollection targetCollection = this.collections.get(i);
            if (collectionTtlCoverInterval(targetCollection, from)) {
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

    private boolean collectionTtlCoverInterval(TimeSeriesCollection collection, long from) {
        long ttl = collection.getTtlMs();
        if (ttl == 0) {
            // housekeeping is disabled
            return true;
        }
        long collectionEnd = System.currentTimeMillis();
        long collectionStart = collectionEnd - ttl;
        return collectionStart <= from;
    }

    public List<Long> getAvailableResolutions() {
        return this.collections.stream().map(TimeSeriesCollection::getResolutionMs).collect(Collectors.toList());
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

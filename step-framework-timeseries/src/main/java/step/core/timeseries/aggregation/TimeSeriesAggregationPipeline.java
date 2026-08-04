package step.core.timeseries.aggregation;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.timeseries.TimeSeriesAggregationConfig;
import step.core.timeseries.TimeSeriesCollection;
import step.core.timeseries.TimeSeriesUtils;
import step.core.timeseries.bucket.Aggregation;
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
    private final int maxAlignmentIntervals;
    private boolean ttlEnabled;

    public TimeSeriesAggregationPipeline(List<TimeSeriesCollection> collections, int responseMaxIntervals, int idealResponseIntervals, boolean ttlEnabled) {
        this(collections, responseMaxIntervals, idealResponseIntervals, TimeSeriesAggregationConfig.DEFAULT_MAX_ALIGNMENT_INTERVALS, ttlEnabled);
    }

    public TimeSeriesAggregationPipeline(List<TimeSeriesCollection> collections, int responseMaxIntervals, int idealResponseIntervals, int maxAlignmentIntervals, boolean ttlEnabled) {
        this.ttlEnabled = ttlEnabled;
        if (responseMaxIntervals <= 0) {
            throw new IllegalArgumentException("responseMaxIntervals must be greater than 0");
        }
        if (idealResponseIntervals <= 0) {
            throw new IllegalArgumentException("idealResponseIntervals must be greater than 0");
        }
        if (maxAlignmentIntervals <= 0) {
            throw new IllegalArgumentException("maxAlignmentIntervals must be greater than 0");
        }
        this.responseMaxIntervals = responseMaxIntervals;
        this.idealResponseIntervals = idealResponseIntervals;
        this.maxAlignmentIntervals = maxAlignmentIntervals;
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
            .setAlignmentResolution(finalParams.getAlignmentResolution())
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
     * <p>
     * When both axes reduce to a scalar, the time aggregation is applied on the alignment grid rather than on the
     * response resolution, and the values the group aggregation produces for each alignment interval are rolled up
     * into the response buckets by the time aggregation. See
     * {@link #calculateAlignmentResolution(TimeSeriesAggregationQuery, long, long, long)}.
     */
    private Map<BucketAttributes, Map<Long, BucketBuilder>> collectByAggregating(TimeSeriesAggregationQuery query, TimeSeriesProcessedParams finalParams, TimeSeriesCollection collection) {
        // True when the alignment grid is finer than the response resolution, i.e. when the group aggregation has to
        // be applied per alignment interval and its values rolled up into the response buckets afterwards
        boolean rollUp = finalParams.getAlignmentResolution() < finalParams.getResolution();

        // Perform time-window aggregation and partition the time series:
        // Aggregate each source series into the aligned intervals and assign the resulting series to their respective groups (defined by the group dimensions).
        // Do not perform any cross-series aggregation at this stage.
        Map<Long, Map<BucketAttributes, Map<BucketAttributes, BucketBuilder>>> alignedSeriesByGroup = new HashMap<>();
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = collection.queryTimeSeries(finalParams)) {
            stream.forEach(bucket -> {
                bucketCount.increment();
                // The attributes of the source series
                BucketAttributes bucketAttributes = bucket.getAttributes() != null ? bucket.getAttributes() : new BucketAttributes();
                // The subset of attributes corresponding to the requested group dimensions (group by)
                BucketAttributes groupAttributes = getGroupAttributes(bucket, finalParams.getGroupDimensions());
                // The index of the alignment interval the current bucket falls into
                long alignmentIndex = calculateAlignmentBeginAnchor(bucket.getBegin(), finalParams);
                // Get or create the alignment interval corresponding to the time index of the current bucket
                Map<BucketAttributes, Map<BucketAttributes, BucketBuilder>> alignmentInterval = alignedSeriesByGroup.computeIfAbsent(alignmentIndex, a -> new HashMap<>());
                // Get or create the group of builders corresponding to the current group (defined by the group dimensions)
                Map<BucketAttributes, BucketBuilder> indexSeriesBuckets = alignmentInterval.computeIfAbsent(groupAttributes, a -> new HashMap<>());
                // Get the builder for the attributes of the current bucket. The full attributes of the series are kept
                // at this stage, so that the attribute collection can be performed on them during the group-by aggregation
                BucketBuilder bucketBuilder = indexSeriesBuckets.computeIfAbsent(bucketAttributes, a -> new BucketBuilder(query.getTimeAggregation(), alignmentIndex, alignmentIndex + finalParams.getAlignmentResolution()).withAttributes(bucketAttributes));
                // Merge the current source bucket into the builder. The configured time-window aggregation is
                // applied when the builder is reduced, at the group-by stage
                bucketBuilder.merge(bucket);
            });
        }
        logAggregationDuration("time-window aggregation", t1, bucketCount, alignedSeriesByGroup);

        // Aggregate the grouped series:
        // For each alignment interval, apply the configured group-by aggregation across the aligned series in each
        // group, and contribute the resulting value to the response bucket the interval belongs to.
        Map<BucketAttributes, Map<Long, BucketBuilder>> resultBuilder = new HashMap<>();
        // For each alignment interval
        alignedSeriesByGroup.forEach((alignmentIndex, alignmentInterval) -> {
            // The response bucket the alignment interval belongs to. The alignment resolution being a divisor of the
            // response resolution, an alignment interval never spans two response buckets
            long resultIndex = calculateBucketBeginAnchor(alignmentIndex, finalParams);
            // For each group
            alignmentInterval.forEach((groupAttributes, group) -> {
                Map<Long, BucketBuilder> resultSeriesBuilder = resultBuilder.computeIfAbsent(groupAttributes, a -> new TreeMap<>());
                BucketBuilder resultBucketBuilder = resultSeriesBuilder.computeIfAbsent(resultIndex, i -> newGroupBucketBuilder(query, finalParams, groupAttributes, i, rollUp));
                if (rollUp) {
                    // Reduce the series of the group into one value for this alignment interval
                    BucketBuilder alignmentBuilder = new BucketBuilder(query.getGroupAggregation(), alignmentIndex, alignmentIndex + finalParams.getAlignmentResolution());
                    group.forEach((seriesAttributes, series) -> {
                        alignmentBuilder.aggregate(series);
                        // The attributes are collected on the result builder, from the source series they belong to.
                        // Collecting them on the intermediate builder would nest the collected values into each other
                        resultBucketBuilder.accumulateAttributes(seriesAttributes);
                    });
                    // Contribute the value of this alignment interval to the response bucket. How it contributes is
                    // defined by the group aggregation the intermediate builder was built with, and how the response
                    // bucket reduces its intervals is defined by the time aggregation
                    resultBucketBuilder.aggregate(alignmentBuilder);
                } else {
                    // For each series of the group
                    group.forEach((seriesAttributes, series) -> {
                        // Aggregate the series into the group. How the series contributes is defined by the
                        // time-window aggregation it was built with
                        resultBucketBuilder.aggregate(series);
                    });
                }
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
        return newGroupBucketBuilder(query, finalParams, groupAttributes, timeSliceIndex, false);
    }

    private BucketBuilder newGroupBucketBuilder(TimeSeriesAggregationQuery query, TimeSeriesProcessedParams finalParams, BucketAttributes groupAttributes, long timeSliceIndex, boolean rollUp) {
        // Without roll-up, the response bucket is the result of the group aggregation applied on the series it holds.
        // With roll-up, it holds one value per alignment interval, produced by the group aggregation, and the time
        // aggregation defines how those values are reduced over time
        Aggregation aggregation = rollUp ? query.getTimeAggregation() : query.getGroupAggregation();
        return new BucketBuilder(aggregation, timeSliceIndex, getBucketEnd(timeSliceIndex, finalParams))
            // The group attributes are copied, so that collecting the attributes doesn't mutate the key of the response
            .withAttributes(new BucketAttributes(groupAttributes))
            .withAccumulateAttributes(query.getCollectAttributeKeys(), query.getCollectAttributesValuesLimit());
    }

    private void logAggregationDuration(String aggregationName, long startTime, LongAdder bucketCount) {
        if (logger.isDebugEnabled()) {
            logger.debug("Performed " + aggregationName + " in " + (System.currentTimeMillis() - startTime) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }
    }

    private void logAggregationDuration(String aggregationName, long startTime, LongAdder bucketCount, Map<Long, Map<BucketAttributes, Map<BucketAttributes, BucketBuilder>>> alignedSeriesByGroup) {
        if (logger.isDebugEnabled()) {
            // The number of builders retained while collecting drives the memory footprint of the aggregation
            long builderCount = alignedSeriesByGroup.values().stream()
                .flatMap(interval -> interval.values().stream())
                .mapToLong(Map::size).sum();
            logger.debug("Performed " + aggregationName + " in " + (System.currentTimeMillis() - startTime) + "ms. Number of buckets processed: " + bucketCount.longValue()
                + ". Number of alignment intervals: " + alignedSeriesByGroup.size() + ". Number of builders retained: " + builderCount);
        }
    }

    private long getBucketEnd(long i, TimeSeriesProcessedParams finalParams) {
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
            .setAlignmentResolution(calculateAlignmentResolution(query, sourceResolution, resultResolution, resultTo - resultFrom, maxAlignmentIntervals))
            .setGroupDimensions(query.getGroupDimensions())
            .setFilter(query.getFilter())
            .setShrink(query.isShrink())
            .setCollectAttributeKeys(query.getCollectAttributeKeys())
            .setCollectAttributesValuesLimit(query.getCollectAttributesValuesLimit());
    }

    /**
     * Determines the resolution of the grid on which the time aggregation is applied.
     * <p>
     * A scalar time aggregation reduces each source series to one single value, whatever the number of source buckets
     * it holds and whatever the part of the time window it actually covers. Applying it over the whole response
     * resolution and only then aggregating the series of a group therefore lets a series which existed during a
     * fraction of the window contribute as if it had existed during the whole window: a group-by SUM over series with
     * disjoint lifetimes ends up summing values which never coexisted. The wider the response bucket, the larger the
     * distortion, up to the number of source buckets it covers.
     * <p>
     * The time aggregation is therefore applied on a finer grid, the group aggregation is applied on that grid too,
     * and the resulting values are rolled up into the response buckets, see
     * {@link #collectByAggregating(TimeSeriesAggregationQuery, TimeSeriesProcessedParams, TimeSeriesCollection)}.
     * The ideal grid is the source resolution, i.e. the finest grid the data allows, but the pipeline holds one
     * builder per alignment interval and source series while collecting, so the number of intervals is bounded by
     * maxAlignmentIntervals. When the budget doesn't allow for a grid finer than the response resolution, the
     * alignment falls back to the response resolution and the aggregation behaves as if no alignment took place.
     * <p>
     * The returned resolution is always a divisor of the response resolution, so that an alignment interval never
     * spans two response buckets.
     *
     * @return the alignment resolution, equal to the response resolution when no intermediate alignment applies
     */
    public static long calculateAlignmentResolution(TimeSeriesAggregationQuery query, long sourceResolution, long resultResolution, long rangeDiff, long maxAlignmentIntervals) {
        Aggregation timeAggregation = query.getTimeAggregation();
        Aggregation groupAggregation = query.getGroupAggregation();
        if (timeAggregation == null || timeAggregation.isMerge()) {
            // A merging time aggregation doesn't reduce the series to a scalar: the group aggregation receives their
            // raw samples, and the samples of a window are exactly the union of the samples of the intervals it
            // covers. The result is therefore the same on any grid, and aligning on a finer one would only increase
            // the memory footprint
            return resultResolution;
        }
        if (groupAggregation == null || groupAggregation.isMerge()) {
            // A merging group aggregation is distorted by the lifetime of the series just like a scalar one is, its
            // resulting bucket holding one sample per series whatever the part of the window each of them covers.
            // Aligning is however not applied here, because the value the alignment intervals would be rolled up with
            // is ambiguous: the time aggregation would reduce the bucket to a scalar and change the type of the
            // response, while merging would redefine the documented population of the bucket, which is one sample per
            // series and not one sample per series and interval
            return resultResolution;
        }
        // The response resolution is always a multiple of the source resolution
        long intervalsPerBucket = resultResolution / sourceResolution;
        if (intervalsPerBucket <= 1) {
            // The response buckets are already as fine as the source data
            return resultResolution;
        }
        long resultBuckets = Math.max(1, divideAndRoundUp(rangeDiff, resultResolution));
        long intervalsBudgetPerBucket = maxAlignmentIntervals / resultBuckets;
        if (intervalsBudgetPerBucket <= 1) {
            // The response buckets alone already exhaust the budget
            return resultResolution;
        }
        // Take the finest grid within the budget which still divides the response resolution
        long factor = smallestDivisorAtLeast(intervalsPerBucket, divideAndRoundUp(intervalsPerBucket, intervalsBudgetPerBucket));
        return factor * sourceResolution;
    }

    private static long smallestDivisorAtLeast(long value, long minDivisor) {
        for (long divisor = Math.max(1, minDivisor); divisor < value; divisor++) {
            if (value % divisor == 0) {
                return divisor;
            }
        }
        return value;
    }

    private static long divideAndRoundUp(long value, long divisor) {
        return (value + divisor - 1) / divisor;
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

    /**
     * @return the begin of the response bucket the given timestamp falls into
     */
    private long calculateBucketBeginAnchor(long bucketBegin, TimeSeriesProcessedParams params) {
        long rangeFrom = params.getFrom();
        if (params.isShrink()) {
            return rangeFrom;
        } else {
            return anchor(bucketBegin, rangeFrom, params.getResolution());
        }
    }

    /**
     * @return the begin of the alignment interval the given timestamp falls into
     */
    private long calculateAlignmentBeginAnchor(long bucketBegin, TimeSeriesProcessedParams params) {
        long rangeFrom = params.getFrom();
        if (params.isShrink() && params.getAlignmentResolution() >= params.getResolution()) {
            return rangeFrom;
        } else {
            return anchor(bucketBegin, rangeFrom, params.getAlignmentResolution());
        }
    }

    private static long anchor(long timestamp, long rangeFrom, long resolution) {
        long distanceFromStart = timestamp - rangeFrom;
        return distanceFromStart - distanceFromStart % resolution + rangeFrom;
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

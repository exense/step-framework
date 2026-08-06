package step.core.timeseries.bucket;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Accumulates samples into one single {@link Bucket}, or into one single {@link ScalarBucket} when built with a
 * scalar {@link Aggregation}.
 * <p>
 * Samples are contributed by {@link #ingest(long)} for one raw value, by {@link #merge(Bucket)} for all the samples
 * of a bucket, and by {@link #aggregate(BucketBuilder)} for the aggregate of another builder. They are always
 * accumulated the same way, whatever the aggregation: the sum, the count, the min, the max and the distribution of
 * this builder always describe the samples it holds. The aggregation only defines how the builder is reduced, i.e.
 * what {@link #getScalarValue()} and {@link #buildAggregate()} return.
 * <p>
 * The samples are accumulated as floating point values, because the scalar a builder is reduced to is not necessarily
 * an integer and is contributed as one single sample to the builder of the next stage. Rounding it there would
 * discard the fractional part of every series before the stages are combined, which for instance turns a group of
 * hundred series each averaging 0.4 into 0 instead of 40. The values are therefore only rounded when a {@link Bucket}
 * is built, i.e. once, at the very end of the pipeline. Raw samples being integers, this is lossless for them.
 * <p>
 * The accumulation of the samples is safe for concurrent use, as required by the ingestion pipeline. The attribute
 * collection enabled by {@link #withAccumulateAttributes(Set, int)} is not, and is only used by the single-threaded
 * aggregation pipeline.
 */
public class BucketBuilder {

    private final long begin;
    private final Long end;
    private BucketAttributes attributes;
    private final Aggregation aggregation;
    private final LongAdder countAdder = new LongAdder();
    private final DoubleAdder sumAdder = new DoubleAdder();
    private final DoubleAccumulator min = new DoubleAccumulator(Math::min, Double.POSITIVE_INFINITY);
    private final DoubleAccumulator max = new DoubleAccumulator(Math::max, Double.NEGATIVE_INFINITY);
    private final Map<Long, LongAdder> distribution = new ConcurrentHashMap<>();
    // TODO Make this configurable
    private final long pclPrecision = 10;
    private Set<String> accumulateAttributeKeys;
    private int accumulateAttributeValuesLimit;
    private long samplingIntervalMs;

    /**
     * Creates a merging builder, i.e. a builder reducing to the {@link Bucket} of the samples it holds.
     */
    public BucketBuilder(long begin) {
        this(Aggregation.MERGE, begin);
    }

    /**
     * Creates a merging builder, i.e. a builder reducing to the {@link Bucket} of the samples it holds.
     */
    public BucketBuilder(long begin, long end) {
        this(Aggregation.MERGE, begin, end);
    }

    /**
     * @param aggregation the aggregation this builder reduces to, see {@link #buildAggregate()}
     */
    public BucketBuilder(Aggregation aggregation, long begin) {
        this.begin = begin;
        this.aggregation = aggregation;
        this.end = null;
    }

    /**
     * @param aggregation the aggregation this builder reduces to, see {@link #buildAggregate()}
     */
    public BucketBuilder(Aggregation aggregation, long begin, long end) {
        this.begin = begin;
        this.end = end;
        this.aggregation = aggregation;
    }

    public BucketBuilder withAttributes(BucketAttributes attributes) {
        this.attributes = attributes;
        return this;
    }

    /**
     * Enables the collection of the attribute values of the contributed buckets and builders: for each of the given
     * keys, the distinct values encountered are collected into the attributes of the resulting bucket.
     *
     * @param accumulateAttributeKeys         the attribute keys to collect
     * @param accumulateAttributeValuesLimit  the maximum number of values collected per key
     */
    public BucketBuilder withAccumulateAttributes(Set<String> accumulateAttributeKeys, int accumulateAttributeValuesLimit) {
        this.accumulateAttributeKeys = accumulateAttributeKeys;
        this.accumulateAttributeValuesLimit = accumulateAttributeValuesLimit;
        // Collected attributes are added on top of the attributes already set on this builder, if any
        if (this.attributes == null) {
            this.attributes = new BucketAttributes();
        }
        return this;
    }

    /**
     * Defines the interval at which the series accumulated by this builder is sampled, as required by
     * {@link Aggregation#SAMPLED_AVG}. Ignored by all the other aggregations.
     *
     * @param samplingIntervalMs the sampling interval in milliseconds
     */
    public BucketBuilder withSamplingInterval(long samplingIntervalMs) {
        this.samplingIntervalMs = samplingIntervalMs;
        return this;
    }

    public static BucketBuilder create(long begin) {
        return new BucketBuilder(begin);
    }

    public static BucketBuilder create(long begin, long end) {
        return new BucketBuilder(begin, end);
    }

    /**
     * Adds one single raw sample to this builder.
     */
    public BucketBuilder ingest(long value) {
        ingestValue(value);
        return this;
    }

    /**
     * Adds one single sample to this builder. Only the samples contributed by a scalar {@link Aggregation} may have a
     * fractional part, raw samples are always integers.
     */
    private void ingestValue(double value) {
        countAdder.increment();
        sumAdder.add(value);
        min.accumulate(value);
        max.accumulate(value);
        // The distribution is indexed by integer values, a fractional sample is therefore held by the bracket of the
        // closest integer
        long bracketedValue = Math.round(value);
        distribution.computeIfAbsent(bracketedValue - bracketedValue % pclPrecision, k -> new LongAdder()).increment();
    }

    /**
     * Merges the given bucket into this builder, i.e. adds all the raw samples it holds. The distribution, the min
     * and the max are merged by union, so that percentiles remain percentiles over the underlying raw samples.
     */
    public BucketBuilder merge(Bucket bucket) {
        countAdder.add(bucket.getCount());
        sumAdder.add(bucket.getSum());
        min.accumulate(bucket.getMin());
        max.accumulate(bucket.getMax());

        Map<Long, Long> bucketDistribution = bucket.getDistribution();
        if (bucketDistribution != null) {
            bucketDistribution.forEach((key, value) ->
                distribution.computeIfAbsent(key, k -> new LongAdder()).add(value));
        }
        accumulateAttributes(bucket.getAttributes());
        return this;
    }

    private void merge(BucketBuilder builder) {
        countAdder.add(builder.getCount());
        sumAdder.add(builder.getSumAsDouble());
        min.accumulate(builder.getMinAsDouble());
        max.accumulate(builder.getMaxAsDouble());
        builder.distribution.forEach((key, value) ->
            distribution.computeIfAbsent(key, k -> new LongAdder()).add(value.longValue()));
        accumulateAttributes(builder.attributes);
    }

    /**
     * Aggregates the given builder into this builder, i.e. contributes its aggregate to this one. How the contribution
     * is made is defined by the aggregation of the contributing builder, not by the one of this builder: a
     * {@link Aggregation#MERGE} builder contributes all the raw samples it holds, while a scalar one contributes its
     * scalar value as one single sample. The attributes of the contributing builder are collected either way.
     */
    public BucketBuilder aggregate(BucketBuilder builder) {
        if (builder.aggregation.isMerge()) {
            merge(builder);
        } else {
            ingestValue(builder.getScalarValue());
            accumulateAttributes(builder.attributes);
        }
        return this;
    }

    /**
     * @return the scalar this builder reduces to, as defined by its aggregation. Not necessarily an integer, see
     * {@link BucketBuilder}
     */
    public double getScalarValue() {
        return aggregation.getValue(this);
    }

    private void accumulateAttributes(BucketAttributes bucketAttr) {
        if (accumulateAttributeKeys != null && bucketAttr != null && !bucketAttr.isEmpty()) {
            accumulateAttributeKeys.forEach(a -> {
                Object value = bucketAttr.get(a);
                if (value != null) {
                    Object currentValue = attributes.get(a);
                    if (currentValue != null && !(currentValue instanceof Set)) {
                        // The key is also a group dimension: its exact value is already set on this builder
                        // and is constant across the group, so there is nothing to collect
                        return;
                    }
                    // TODO: we currently misuse the attributes field to return the collected attribute values.
                    //  We should introduce a dedicated field to collect the attribute values
                    Set values = (Set) attributes.computeIfAbsent(a, i -> new HashSet());
                    if (values.size() < accumulateAttributeValuesLimit) {
                        values.add(value);
                    }
                }
            });
        }
    }

    public long getBegin() {
        return begin;
    }

    public Long getEnd() {
        return end;
    }

    /**
     * @return the number of samples accumulated so far
     */
    public long getCount() {
        return countAdder.longValue();
    }

    /**
     * @return the sum of the samples accumulated so far, rounded
     */
    public long getSum() {
        return Math.round(getSumAsDouble());
    }

    /**
     * @return the sum of the samples accumulated so far
     */
    public double getSumAsDouble() {
        return sumAdder.doubleValue();
    }

    /**
     * @return the average of the samples accumulated so far, rounded, 0 if this builder is empty
     */
    public long getAverage() {
        return Math.round(getAverageAsDouble());
    }

    /**
     * @return the average of the samples accumulated so far, 0 if this builder is empty
     */
    public double getAverageAsDouble() {
        long count = getCount();
        return count > 0 ? getSumAsDouble() / count : 0;
    }

    /**
     * Averages the samples accumulated so far over the number of samples this builder is expected to hold, i.e. over
     * the number of sampling intervals its time window covers. The sampling intervals holding no sample therefore
     * count as zero, which is what their absence means for a series sampled at a fixed interval: the series simply
     * didn't exist at that time.
     * <p>
     * @return the average of the samples accumulated so far over the expected number of samples, 0 if this builder is
     * empty
     * @see Aggregation#SAMPLED_AVG
     */
    public double getSampledAverage() {
        long expectedSampleCount = getExpectedSampleCount();
        return expectedSampleCount > 0 ? getSumAsDouble() / expectedSampleCount : 0;
    }

    /**
     * A series existing during the whole window holds one sample per sampling interval the window covers, which is
     * the number of samples this builder is expected to hold. A window which isn't a whole number of sampling
     * intervals doesn't have such a number: it holds one sample more or less depending on where the sampling instants
     * fall, and no divisor reduces both of these counts to the value of the series. The samples are then averaged
     * over their own number, i.e. the sampled average amounts to the plain one.
     * <p>
     * The aggregation pipeline resolves the response resolution to a common multiple of the sampling interval and of
     * the source resolution, which is a whole number of sampling intervals whether the two divide each other or not.
     * This therefore only applies to a builder used outside of the pipeline, or built without a window.
     *
     * @return the number of samples the window of this builder is expected to hold, the number of samples it actually
     * holds if its window isn't a whole number of sampling intervals
     */
    private long getExpectedSampleCount() {
        if (end == null || samplingIntervalMs <= 0 || (end - begin) % samplingIntervalMs != 0) {
            return getCount();
        }
        return (end - begin) / samplingIntervalMs;
    }

    /**
     * @return the lowest sample accumulated so far, rounded, {@link Long#MAX_VALUE} if this builder is empty
     */
    public long getMin() {
        return Math.round(getMinAsDouble());
    }

    /**
     * @return the lowest sample accumulated so far, {@link Double#POSITIVE_INFINITY} if this builder is empty
     */
    public double getMinAsDouble() {
        return min.get();
    }

    /**
     * @return the highest sample accumulated so far, rounded, {@link Long#MIN_VALUE} if this builder is empty
     */
    public long getMax() {
        return Math.round(getMaxAsDouble());
    }

    /**
     * @return the highest sample accumulated so far, {@link Double#NEGATIVE_INFINITY} if this builder is empty
     */
    public double getMaxAsDouble() {
        return max.get();
    }

    /**
     * @return the aggregate of this builder: the merged {@link Bucket} for {@link Aggregation#MERGE}, a
     * {@link ScalarBucket} holding the scalar value of the aggregation otherwise
     */
    public Bucket buildAggregate() {
        return aggregation.isMerge() ? build() : buildScalarBucket();
    }

    private ScalarBucket buildScalarBucket() {
        // The scalar is rounded here and only here, so that the fractional part of the value each stage reduces to is
        // preserved until the end of the pipeline
        ScalarBucket bucket = new ScalarBucket(Math.round(getScalarValue()));
        bucket.setBegin(begin);
        bucket.setEnd(end);
        bucket.setAttributes(attributes);
        return bucket;
    }

    /**
     * Builds the {@link Bucket} of the samples accumulated by this builder, whatever its aggregation. Use
     * {@link #buildAggregate()} to obtain the aggregate defined by the aggregation instead.
     */
    public Bucket build() {
        Bucket bucket = new Bucket();
        bucket.setBegin(begin);
        bucket.setEnd(end);
        bucket.setAttributes(attributes);
        bucket.setCount(getCount());
        bucket.setSum(getSum());
        bucket.setMin(getMin());
        bucket.setMax(getMax());
        bucket.setPclPrecision(pclPrecision);
        bucket.setDistribution(distribution.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue())));
        return bucket;
    }
}

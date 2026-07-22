package step.core.timeseries.bucket;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
    private final LongAdder sumAdder = new LongAdder();
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final Map<Long, LongAdder> distribution = new ConcurrentHashMap<>();
    // TODO Make this configurable
    private final long pclPrecision = 10;
    private Set<String> accumulateAttributeKeys;
    private int accumulateAttributeValuesLimit;

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

    public static BucketBuilder create(long begin) {
        return new BucketBuilder(Aggregation.MERGE, begin);
    }

    public static BucketBuilder create(long begin, long end) {
        return new BucketBuilder(Aggregation.MERGE, begin, end);
    }

    /**
     * Adds one single raw sample to this builder.
     */
    public BucketBuilder ingest(long value) {
        countAdder.increment();
        sumAdder.add(value);
        updateMin(value);
        updateMax(value);
        distribution.computeIfAbsent(value - value % pclPrecision, k -> new LongAdder()).increment();
        return this;
    }

    /**
     * Merges the given bucket into this builder, i.e. adds all the raw samples it holds. The distribution, the min
     * and the max are merged by union, so that percentiles remain percentiles over the underlying raw samples.
     */
    public BucketBuilder merge(Bucket bucket) {
        countAdder.add(bucket.getCount());
        sumAdder.add(bucket.getSum());
        updateMin(bucket.getMin());
        updateMax(bucket.getMax());

        Map<Long, Long> bucketDistribution = bucket.getDistribution();
        if (bucketDistribution != null) {
            bucketDistribution.forEach((key, value) ->
                distribution.computeIfAbsent(key, k -> new LongAdder()).add(value));
        }
        accumulateAttributes(bucket.getAttributes());
        return this;
    }

    /**
     * Aggregates the given builder into this builder, i.e. contributes its aggregate to this one. How the contribution
     * is made is defined by the aggregation of the contributing builder, not by the one of this builder: a
     * {@link Aggregation#MERGE} builder contributes all the raw samples it holds, while a scalar one contributes its
     * scalar value as one single sample. The attributes of the contributing builder are collected either way.
     */
    public BucketBuilder aggregate(BucketBuilder builder) {
        if (builder.aggregation.isMerge()) {
            merge(builder.build());
        } else {
            ingest(builder.getScalarValue());
            accumulateAttributes(builder.attributes);
        }
        return this;
    }

    /**
     * @return the scalar this builder reduces to, as defined by its aggregation
     */
    public long getScalarValue() {
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

    private void updateMin(long value) {
        min.updateAndGet(curMin -> Math.min(value, curMin));
    }

    private void updateMax(long value) {
        max.updateAndGet(curMax -> Math.max(value, curMax));
    }

    /**
     * @return the number of samples accumulated so far
     */
    public long getCount() {
        return countAdder.longValue();
    }

    /**
     * @return the sum of the samples accumulated so far
     */
    public long getSum() {
        return sumAdder.longValue();
    }

    /**
     * @return the average of the samples accumulated so far, 0 if this builder is empty
     */
    public long getAverage() {
        long count = getCount();
        return count > 0 ? getSum() / count : 0;
    }

    /**
     * @return the lowest sample accumulated so far, {@link Long#MAX_VALUE} if this builder is empty
     */
    public long getMin() {
        return min.get();
    }

    /**
     * @return the highest sample accumulated so far, {@link Long#MIN_VALUE} if this builder is empty
     */
    public long getMax() {
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
        ScalarBucket bucket = new ScalarBucket(getScalarValue());
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
        bucket.setCount(countAdder.longValue());
        bucket.setSum(sumAdder.longValue());
        bucket.setMin(min.longValue());
        bucket.setMax(max.longValue());
        bucket.setPclPrecision(pclPrecision);
        bucket.setDistribution(distribution.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue())));
        return bucket;
    }
}

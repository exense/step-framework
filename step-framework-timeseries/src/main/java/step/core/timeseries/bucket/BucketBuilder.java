package step.core.timeseries.bucket;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

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


    public BucketBuilder(Aggregation aggregation, long begin) {
        this.begin = begin;
        this.aggregation = aggregation;
        this.end = null;
    }

    public BucketBuilder(Aggregation aggregation, long begin, long end) {
        this.begin = begin;
        this.end = end;
        this.aggregation = aggregation;
    }

    public BucketBuilder withAttributes(BucketAttributes attributes) {
        this.attributes = attributes;
        return this;
    }

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
        sumAdder.add(bucket.getSum());
        countAdder.add(bucket.getCount());
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

    public long getCount() {
        return countAdder.longValue();
    }

    public long getSum() {
        return sumAdder.longValue();
    }

    public long getAverage() {
        long count = getCount();
        return count > 0 ? getSum() / count : 0;
    }

    public long getMin() {
        return min.get();
    }

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

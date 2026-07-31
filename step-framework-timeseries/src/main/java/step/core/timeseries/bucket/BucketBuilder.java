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
    private final LongAdder countAdder = new LongAdder();
    private final LongAdder sumAdder = new LongAdder();
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final Map<Long, LongAdder> distribution = new ConcurrentHashMap<>();
    // TODO Make this configurable
    private final long pclPrecision = 10;
    private Set<String> accumulateAttributeKeys;
    private int accumulateAttributeValuesLimit;


    public BucketBuilder(long begin) {
        this.begin = begin;
        this.end = null;
    }

    public BucketBuilder(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    public BucketBuilder withAttributes(BucketAttributes attributes) {
        this.attributes = attributes;
        return this;
    }

    public BucketBuilder withAccumulateAttributes(Set<String> accumulateAttributeKeys, int accumulateAttributeValuesLimit) {
        this.accumulateAttributeKeys = accumulateAttributeKeys;
        this.accumulateAttributeValuesLimit = accumulateAttributeValuesLimit;
        this.attributes = new BucketAttributes();
        return this;
    }

    public static BucketBuilder create(long begin) {
        return new BucketBuilder(begin);
    }

    public BucketBuilder ingest(long value) {
        countAdder.increment();
        sumAdder.add(value);
        updateMin(value);
        updateMax(value);
        distribution.computeIfAbsent(value - value % pclPrecision, k -> new LongAdder()).increment();
        return this;
    }

    public BucketBuilder accumulate(Bucket bucket) {
        countAdder.add(bucket.getCount());
        sumAdder.add(bucket.getSum());
        updateMin(bucket.getMin());
        updateMax(bucket.getMax());

        Map<Long, Long> bucketDistribution = bucket.getDistribution();
        if (bucketDistribution != null) {
            bucketDistribution.forEach((key, value) ->
                distribution.computeIfAbsent(key, k -> new LongAdder()).add(value));
        }
        accumulateAttributes(bucket);
        return this;
    }

    private void accumulateAttributes(Bucket bucket) {
        BucketAttributes bucketAttr = bucket.getAttributes();
        if (accumulateAttributeKeys != null && bucketAttr != null && !bucketAttr.isEmpty()) {
            accumulateAttributeKeys.forEach(a -> {
                Object value = bucketAttr.get(a);
                if (value != null) {
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
        return count > 0 ? Math.round((1.0 * getSum()) / count) : 0;
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

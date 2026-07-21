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
    private final LongAdder contributingCountAdder = new LongAdder();
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
        // Collected attributes are added on top of the attributes already set on this builder, if any
        if (this.attributes == null) {
            this.attributes = new BucketAttributes();
        }
        return this;
    }

    public static BucketBuilder create(long begin) {
        return new BucketBuilder(begin);
    }

    public BucketBuilder ingest(long value) {
        contributingCountAdder.increment();
        countAdder.increment();
        sumAdder.add(value);
        updateMin(value);
        updateMax(value);
        distribution.computeIfAbsent(value - value % pclPrecision, k -> new LongAdder()).increment();
        return this;
    }

    /**
     * Aggregates the given bucket into this builder using the provided aggregation.
     * <p>
     * The aggregation only drives the scalar accumulated into the sum and the weight of that contribution. The
     * distribution, the min and the max are always merged by union, independently of the aggregation, so that
     * percentiles remain percentiles over the underlying raw samples. Likewise the raw sample count is always
     * carried over, so that no aggregation loses track of how many samples the result is based on.
     */
    public BucketBuilder aggregate(Bucket bucket, Aggregation aggregation) {
        contributingCountAdder.add(aggregation.getWeight(bucket));
        sumAdder.add(aggregation.getValue(bucket));
        countAdder.add(bucket.getCount());
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

    /**
     * Merges the given bucket into this builder, i.e. adds all the raw samples it holds.
     * Equivalent to {@link #aggregate(Bucket, Aggregation)} with {@link Aggregation#AVG}.
     */
    public BucketBuilder accumulate(Bucket bucket) {
        return aggregate(bucket, Aggregation.AVG);
    }

    private void accumulateAttributes(Bucket bucket) {
        BucketAttributes bucketAttr = bucket.getAttributes();
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

    public Bucket build() {
        Bucket bucket = new Bucket();
        bucket.setBegin(begin);
        bucket.setEnd(end);
        bucket.setAttributes(attributes);
        bucket.setContributorCount(contributingCountAdder.longValue());
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

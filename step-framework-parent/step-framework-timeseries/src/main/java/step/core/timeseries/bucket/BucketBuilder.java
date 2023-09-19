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
    private List<String> accumulateAttributeKeys;
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

    public BucketBuilder withAccumulateAttributes(List<String> accumulateAttributeKeys, int accumulateAttributeValuesLimit) {
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
        if (accumulateAttributeKeys != null && !accumulateAttributeKeys.isEmpty() && bucketAttr != null && !bucketAttr.isEmpty()) {
            accumulateAttributeKeys.forEach(a -> {
                if (bucketAttr.containsKey(a)) {
                    Set values = (Set) attributes.computeIfAbsent(a, i -> new HashSet());
                    if (values.size() < accumulateAttributeValuesLimit) {
                        values.add(bucketAttr.get(a));
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

package step.core.timeseries;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class BucketBuilder {

    private final long begin;
    private Map<String, String> attributes;
    private final LongAdder countAdder = new LongAdder();
    private final LongAdder sumAdder = new LongAdder();
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final Map<Long, LongAdder> distribution = new ConcurrentHashMap<>();
    // TODO Make this configurable
    private final long pclPrecision = 10;

    public BucketBuilder(long begin) {
        this.begin = begin;
    }

    public BucketBuilder withAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
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
        return this;
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

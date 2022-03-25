package step.core.timeseries;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class BucketBuilder {

    private final LongAdder countAdder = new LongAdder();
    private final LongAdder sumAdder = new LongAdder();
    private final AtomicLong min = new AtomicLong();
    private final AtomicLong max = new AtomicLong();
    private final Map<Long, LongAdder> distribution = new ConcurrentHashMap<>();
    private final long pclPrecision = 1;

    public static BucketBuilder create() {
        return new BucketBuilder();
    }

    public void injest(long value) {
        countAdder.increment();
        sumAdder.add(value);
        updateMin(value);
        updateMax(value);
        distribution.computeIfAbsent(value - value % pclPrecision, k -> new LongAdder()).increment();
    }

    public void accumulate(Bucket bucket) {
        countAdder.add(bucket.getCount());
        sumAdder.add(bucket.getSum());
        updateMin(bucket.getMin());
        updateMax(bucket.getMax());

        Map<Long, Long> bucketDistribution = bucket.getDistribution();
        if (bucketDistribution != null) {
            bucketDistribution.forEach((key, value) ->
                    distribution.computeIfAbsent(key, k -> new LongAdder()).add(value));
        }
    }

    private void updateMin(long value) {
        min.updateAndGet(curMin -> Math.min(value, curMin));
    }

    private void updateMax(long value) {
        max.updateAndGet(curMax -> Math.max(value, curMax));
    }

    public Bucket build() {
        Bucket bucket = new Bucket();
        bucket.setCount(countAdder.longValue());
        bucket.setSum(sumAdder.longValue());
        bucket.setMin(min.longValue());
        bucket.setMax(max.longValue());
        bucket.setDistribution(distribution.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue())));
        return bucket;
    }

}

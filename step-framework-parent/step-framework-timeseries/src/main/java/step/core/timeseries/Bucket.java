package step.core.timeseries;

import step.core.accessors.AbstractIdentifiableObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class Bucket extends AbstractIdentifiableObject {

    private long begin;
    private BucketAttributes attributes;
    private long count, sum, min, max;
    private long pclPrecision;
    private Map<Long, Long> distribution;

    public Bucket() {
    }

    public Bucket(long begin) {
        this.begin = begin;
    }

    public long getBegin() {
        return begin;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public BucketAttributes getAttributes() {
        return attributes;
    }

    public void setAttributes(BucketAttributes attributes) {
        this.attributes = attributes;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public Map<Long, Long> getDistribution() {
        return distribution;
    }

    public long getPclPrecision() {
        return pclPrecision;
    }

    public void setPclPrecision(long pclPrecision) {
        this.pclPrecision = pclPrecision;
    }

    public void setDistribution(Map<Long, Long> distribution) {
        this.distribution = distribution;
    }

    public long getPercentile(int percentile) {
        long numberOfPointsAtPercentile = (long) (1.0 * percentile / 100 * count);
        LongAdder count = new LongAdder();
        long percentileValue = 0;

        ArrayList<Long> keys = new ArrayList<>(distribution.keySet());
        Collections.sort(keys);
        for (Long key : keys) {
            Long value = distribution.get(key);
            count.add(value);
            percentileValue = key;
            if (count.longValue() >= numberOfPointsAtPercentile) {
                break;
            }
        }
        return percentileValue;
    }
}

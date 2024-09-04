package step.core.timeseries.bucket;

import step.core.accessors.AbstractIdentifiableObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class Bucket extends AbstractIdentifiableObject {

    private long begin;
    private Long end;
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

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
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

    public long getPercentile(double percentile) {
        if (percentile < 0.0 || percentile > 100.0) {
            throw new IllegalArgumentException("Percentile must be between 0.0 and 100.0");
        }
        long numberOfPointsAtPercentile = (long) Math.ceil(percentile / 100 * count);
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

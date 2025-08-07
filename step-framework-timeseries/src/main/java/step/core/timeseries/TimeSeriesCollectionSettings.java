package step.core.timeseries;

import java.util.Set;

public class TimeSeriesCollectionSettings {

    private long resolution;
    private long ttl;
    private long ingestionFlushingPeriodMs;
    private int ingestionFlushAsyncQueueSize;
    private int ingestionFlushSeriesQueueSize;
    private Set<String> ignoredAttributes;

    public long getResolution() {
        return resolution;
    }

    public long getTtl() {
        return ttl;
    }

    public long getIngestionFlushingPeriodMs() {
        return ingestionFlushingPeriodMs;
    }

    public TimeSeriesCollectionSettings setResolution(long resolution) {
        this.resolution = resolution;
        return this;
    }

    public TimeSeriesCollectionSettings setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public TimeSeriesCollectionSettings setIngestionFlushingPeriodMs(long ingestionFlushingPeriodMs) {
        this.ingestionFlushingPeriodMs = ingestionFlushingPeriodMs;
        return this;
    }

    public int getIngestionFlushSeriesQueueSize() {
        return ingestionFlushSeriesQueueSize;
    }

    public TimeSeriesCollectionSettings setIngestionFlushSeriesQueueSize(int ingestionFlushSeriesQueueSize) {
        this.ingestionFlushSeriesQueueSize = ingestionFlushSeriesQueueSize;
        return this;
    }

    public int getIngestionFlushAsyncQueueSize() {
        return ingestionFlushAsyncQueueSize;
    }

    public TimeSeriesCollectionSettings setIngestionFlushAsyncQueueSize(int ingestionFlushAsyncQueueSize) {
        this.ingestionFlushAsyncQueueSize = ingestionFlushAsyncQueueSize;
        return this;
    }

    public Set<String> getIgnoredAttributes() {
        return ignoredAttributes;
    }

    public TimeSeriesCollectionSettings setIgnoredAttributes(Set<String> ignoredAttributes) {
        this.ignoredAttributes = ignoredAttributes;
        return this;
    }
}

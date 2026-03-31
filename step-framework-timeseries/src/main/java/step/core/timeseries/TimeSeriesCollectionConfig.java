package step.core.timeseries;

import java.util.Set;

public class TimeSeriesCollectionConfig {

    private long resolution;
    private long ttl;
    private long ingestionFlushingPeriodMs;
    private long ingestionFlushOffsetMs = 10000; // buckets created in the last flushOffsetMs will not be flushed during periodic flush
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

    public TimeSeriesCollectionConfig setResolution(long resolution) {
        this.resolution = resolution;
        return this;
    }

    public TimeSeriesCollectionConfig setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public TimeSeriesCollectionConfig setIngestionFlushingPeriodMs(long ingestionFlushingPeriodMs) {
        this.ingestionFlushingPeriodMs = ingestionFlushingPeriodMs;
        return this;
    }

    public long getIngestionFlushOffsetMs() {
        return ingestionFlushOffsetMs;
    }

    public TimeSeriesCollectionConfig setIngestionFlushOffsetMs(long ingestionFlushOffsetMs) {
        this.ingestionFlushOffsetMs = ingestionFlushOffsetMs;
        return this;
    }

    public int getIngestionFlushSeriesQueueSize() {
        return ingestionFlushSeriesQueueSize;
    }

    public TimeSeriesCollectionConfig setIngestionFlushSeriesQueueSize(int ingestionFlushSeriesQueueSize) {
        this.ingestionFlushSeriesQueueSize = ingestionFlushSeriesQueueSize;
        return this;
    }

    public int getIngestionFlushAsyncQueueSize() {
        return ingestionFlushAsyncQueueSize;
    }

    public TimeSeriesCollectionConfig setIngestionFlushAsyncQueueSize(int ingestionFlushAsyncQueueSize) {
        this.ingestionFlushAsyncQueueSize = ingestionFlushAsyncQueueSize;
        return this;
    }

    public Set<String> getIgnoredAttributes() {
        return ignoredAttributes;
    }

    public TimeSeriesCollectionConfig setIgnoredAttributes(Set<String> ignoredAttributes) {
        this.ignoredAttributes = ignoredAttributes;
        return this;
    }
}

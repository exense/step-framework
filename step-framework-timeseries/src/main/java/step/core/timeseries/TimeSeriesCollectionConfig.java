package step.core.timeseries;

import java.util.Set;

import static step.core.timeseries.TimeSeriesConfig.DEFAULT_FLUSH_OFFSET_MS;

public class TimeSeriesCollectionConfig {

    private long resolutionMs;
    private long ttlMs;
    private long ingestionFlushingPeriodMs;
    // Grace period for the periodic flush thread: a bucket is only flushed once its end time
    // (bucket start + resolution) is at least ingestionFlushOffsetMs in the past. This ensures
    // that late-arriving data points whose timestamp falls within an already-ended bucket are
    // still captured before it is written to the collection.
    private long ingestionFlushOffsetMs = DEFAULT_FLUSH_OFFSET_MS;
    private int ingestionFlushAsyncQueueSize;
    private int ingestionFlushSeriesQueueSize;
    private Set<String> ignoredAttributes;

    public long getResolutionMs() {
        return resolutionMs;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public long getIngestionFlushingPeriodMs() {
        return ingestionFlushingPeriodMs;
    }

    public TimeSeriesCollectionConfig setResolutionMs(long resolutionMs) {
        this.resolutionMs = resolutionMs;
        return this;
    }

    public TimeSeriesCollectionConfig setTtlMs(long ttlMs) {
        this.ttlMs = ttlMs;
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

package step.core.timeseries.ingestion;

import java.util.Set;

public class TimeSeriesIngestionPipelineSettings {

    private long resolution;
    private long flushingPeriodMs;
    private int flushSeriesQueueSize;
    private int flushAsyncQueueSize;
    private Set<String> ignoredAttributes;
    private TimeSeriesIngestionPipeline nextPipeline; // optional

    public long getResolution() {
        return resolution;
    }

    public long getFlushingPeriodMs() {
        return flushingPeriodMs;
    }

    public TimeSeriesIngestionPipelineSettings setResolution(long resolution) {
        this.resolution = resolution;
        return this;
    }

    public TimeSeriesIngestionPipelineSettings setFlushingPeriodMs(long flushingPeriodMs) {
        this.flushingPeriodMs = flushingPeriodMs;
        return this;
    }

    public int getFlushSeriesQueueSize() {
        return flushSeriesQueueSize;
    }

    public TimeSeriesIngestionPipelineSettings setFlushSeriesQueueSize(int flushSeriesQueueSize) {
        this.flushSeriesQueueSize = flushSeriesQueueSize;
        return this;
    }

    public int getFlushAsyncQueueSize() {
        return flushAsyncQueueSize;
    }

    public TimeSeriesIngestionPipelineSettings setFlushAsyncQueueSize(int flushAsyncQueueSize) {
        this.flushAsyncQueueSize = flushAsyncQueueSize;
        return this;
    }

    public TimeSeriesIngestionPipelineSettings setNextPipeline(TimeSeriesIngestionPipeline nextPipeline) {
        this.nextPipeline = nextPipeline;
        return this;
    }

    public TimeSeriesIngestionPipeline getNextPipeline() {
        return nextPipeline;
    }

    public Set<String> getIgnoredAttributes() {
        return ignoredAttributes;
    }

    public TimeSeriesIngestionPipelineSettings setIgnoredAttributes(Set<String> ignoredAttributes) {
        this.ignoredAttributes = ignoredAttributes;
        return this;
    }
}

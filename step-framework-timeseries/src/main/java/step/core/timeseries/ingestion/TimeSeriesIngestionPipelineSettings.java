package step.core.timeseries.ingestion;

import step.core.timeseries.bucket.Bucket;

import java.util.function.Consumer;

public class TimeSeriesIngestionPipelineSettings {

    private long resolution;
    private long flushingPeriodMs;
    private Consumer<Bucket> flushCallback; // optional callback to be triggered when a bucket is flushed
    private TimeSeriesIngestionPipeline nextPipeline; // optional

    public long getResolution() {
        return resolution;
    }

    public long getFlushingPeriodMs() {
        return flushingPeriodMs;
    }

    public Consumer<Bucket> getFlushCallback() {
        return flushCallback;
    }

    public TimeSeriesIngestionPipelineSettings setResolution(long resolution) {
        this.resolution = resolution;
        return this;
    }

    public TimeSeriesIngestionPipelineSettings setFlushingPeriodMs(long flushingPeriodMs) {
        this.flushingPeriodMs = flushingPeriodMs;
        return this;
    }

    public TimeSeriesIngestionPipelineSettings setFlushCallback(Consumer<Bucket> flushCallback) {
        this.flushCallback = flushCallback;
        return this;
    }

    public TimeSeriesIngestionPipelineSettings setNextPipeline(TimeSeriesIngestionPipeline nextPipeline) {
        this.nextPipeline = nextPipeline;
        return this;
    }

    public TimeSeriesIngestionPipeline getNextPipeline() {
        return nextPipeline;
    }
}

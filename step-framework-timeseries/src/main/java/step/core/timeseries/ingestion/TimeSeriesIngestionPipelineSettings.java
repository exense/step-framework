package step.core.timeseries.ingestion;

public class TimeSeriesIngestionPipelineSettings {

    private long resolution;
    private long flushingPeriodMs;
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

    public TimeSeriesIngestionPipelineSettings setNextPipeline(TimeSeriesIngestionPipeline nextPipeline) {
        this.nextPipeline = nextPipeline;
        return this;
    }

    public TimeSeriesIngestionPipeline getNextPipeline() {
        return nextPipeline;
    }
}

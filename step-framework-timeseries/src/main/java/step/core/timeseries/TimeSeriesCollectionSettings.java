package step.core.timeseries;

public class TimeSeriesCollectionSettings {

    private long resolution;
    private long ttl;
    private long ingestionFlushingPeriodMs;

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

}

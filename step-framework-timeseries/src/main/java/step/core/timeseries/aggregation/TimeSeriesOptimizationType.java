package step.core.timeseries.aggregation;

public enum TimeSeriesOptimizationType {
    /**
     * time-series will try to find the aggregate data with the smallest volume (e.g week resolution).
     */
    MOST_EFFICIENT,

    /**
     * time-series will try to use the resolution with the highest granularity, in order to make sure that the data is
     * detailed and has been flushed.
     */
    MOST_ACCURATE
}

package step.core.timeseries.aggregation;

import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.List;
import java.util.Map;

public class TimeSeriesAggregationResponse {

    private List<Long> axis;
    private final Map<BucketAttributes, Map<Long, Bucket>> series;

    private final long resolution;

    public TimeSeriesAggregationResponse(Map<BucketAttributes, Map<Long, Bucket>> series, long resolution) {
        this.series = series;
        this.resolution = resolution;
    }

    public TimeSeriesAggregationResponse withAxis(List<Long> axis) {
        this.axis = axis;
        return this;
    }

    public List<Long> getAxis() {
        return axis;
    }

    public long getResolution() {
        return resolution;
    }

    public Map<BucketAttributes, Map<Long, Bucket>> getSeries() {
        return series;
    }

    public Map<Long, Bucket> getFirstSeries() {
        return series.values().stream().findFirst().get();
    }
}

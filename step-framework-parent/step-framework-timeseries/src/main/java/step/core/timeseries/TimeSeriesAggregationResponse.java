package step.core.timeseries;

import java.util.List;
import java.util.Map;

public class TimeSeriesAggregationResponse {

    private List<Long> axis;
    private Map<BucketAttributes, Map<Long, Bucket>> series;

    protected TimeSeriesAggregationResponse(Map<BucketAttributes, Map<Long, Bucket>> series) {
        this.series = series;
    }

    protected TimeSeriesAggregationResponse withAxis(List<Long> axis) {
        this.axis = axis;
        return this;
    }

    public List<Long> getAxis() {
        return axis;
    }

    public Map<BucketAttributes, Map<Long, Bucket>> getSeries() {
        return series;
    }

    public Map<Long, Bucket> getFirstSeries() {
        return series.values().stream().findFirst().get();
    }
}

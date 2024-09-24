package step.core.timeseries.aggregation;

import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.List;
import java.util.Map;

public class TimeSeriesAggregationResponse {

    private List<Long> axis;
    private final Map<BucketAttributes, Map<Long, Bucket>> series;

    /**
     * Resolution used in the final result series
     */
    private final long resolution;

    /**
     * The resolution of the collection that was used to create this time-series.
     */
    private final long collectionResolution;

    /**
     * Set to true when data for the ideal resolution was not found, and a higher resolution was used instead.
     */
    private final boolean higherResolutionUsed;

    public TimeSeriesAggregationResponse(Map<BucketAttributes, Map<Long, Bucket>> series, long resolution, long collectionResolution, boolean higherResolutionUsed) {
        this.series = series;
        this.resolution = resolution;
        this.collectionResolution = collectionResolution;
        this.higherResolutionUsed = higherResolutionUsed;
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

    public boolean isHigherResolutionUsed() {
        return higherResolutionUsed;
    }

    public long getCollectionResolution() {
        return collectionResolution;
    }
}

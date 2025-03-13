package step.core.timeseries.aggregation;

import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TimeSeriesAggregationResponseBuilder {

    private Long start;
    private Long end;
    private Map<BucketAttributes, Map<Long, Bucket>> series;
    private long resolution;
    private long collectionResolution;
    private Set<String> collectionIgnoredAttributes;
    private boolean higherResolutionUsed;
    private boolean ttlCovered;

    public long getStart() {
        return start;
    }

    public TimeSeriesAggregationResponseBuilder setStart(long start) {
        this.start = start;
        return this;
    }

    public Map<BucketAttributes, Map<Long, Bucket>> getSeries() {
        return series;
    }

    public long getEnd() {
        return end;
    }

    public TimeSeriesAggregationResponseBuilder setEnd(long end) {
        this.end = end;
        return this;
    }

    public TimeSeriesAggregationResponseBuilder setSeries(Map<BucketAttributes, Map<Long, Bucket>> series) {
        this.series = series;
        return this;
    }

    public long getResolution() {
        return resolution;
    }

    public TimeSeriesAggregationResponseBuilder setResolution(long resolution) {
        this.resolution = resolution;
        return this;
    }

    public long getCollectionResolution() {
        return collectionResolution;
    }

    public TimeSeriesAggregationResponseBuilder setCollectionResolution(long collectionResolution) {
        this.collectionResolution = collectionResolution;
        return this;
    }

    public boolean isHigherResolutionUsed() {
        return higherResolutionUsed;
    }

    public TimeSeriesAggregationResponseBuilder setHigherResolutionUsed(boolean higherResolutionUsed) {
        this.higherResolutionUsed = higherResolutionUsed;
        return this;
    }

    public TimeSeriesAggregationResponseBuilder setTtlCovered(boolean ttlCovered) {
        this.ttlCovered = ttlCovered;
        return this;
    }

    public Set<String> getCollectionIgnoredAttributes() {
        return collectionIgnoredAttributes;
    }

    public TimeSeriesAggregationResponseBuilder setCollectionIgnoredAttributes(Set<String> collectionIgnoredAttributes) {
        this.collectionIgnoredAttributes = collectionIgnoredAttributes;
        return this;
    }

    public TimeSeriesAggregationResponse build() {
        Objects.requireNonNull(start);
        Objects.requireNonNull(end);
        return new TimeSeriesAggregationResponse(series, start, end, resolution, collectionResolution, collectionIgnoredAttributes, higherResolutionUsed, ttlCovered);
    }
}

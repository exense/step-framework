package step.core.timeseries.aggregation;

import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeSeriesAggregationResponse {

    private long start;
    private long end;
    private final Map<BucketAttributes, Map<Long, Bucket>> series;

    /**
     * Resolution used in the final result series
     */
    private final long resolution;

    /**
     * The resolution of the collection that was used to create this time-series.
     */
    private final long collectionResolution;

    private final Set<String> collectionIgnoredAttributes;

    /**
     * Set to true when data for the ideal resolution was not found, and a higher resolution was used instead.
     */
    private final boolean higherResolutionUsed;

    /**
     * Set to false when the requested range data was partially or entirely deleted.
     */
    private final boolean ttlCovered;

    TimeSeriesAggregationResponse(Map<BucketAttributes, Map<Long, Bucket>> series, long start, long end, long resolution, long collectionResolution, Set<String> collectionIgnoredAttributes, boolean higherResolutionUsed, boolean ttlCovered) {
        this.series = series;
        this.resolution = resolution;
        this.collectionResolution = collectionResolution;
        this.collectionIgnoredAttributes = collectionIgnoredAttributes;
        this.higherResolutionUsed = higherResolutionUsed;
        this.start = start;
        this.end = end;
        this.ttlCovered = ttlCovered;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
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

    public Set<String> getCollectionIgnoredAttributes() {
        return collectionIgnoredAttributes;
    }

    public boolean isTtlCovered() {
        return ttlCovered;
    }
}

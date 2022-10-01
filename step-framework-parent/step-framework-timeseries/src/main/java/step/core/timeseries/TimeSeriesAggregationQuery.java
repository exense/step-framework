package step.core.timeseries;

import java.util.*;
import java.util.function.Function;

public class TimeSeriesAggregationQuery {

    private final Function<Long, Long> indexProjectionFunction;

    // Range
    private Long bucketIndexFrom;
    private Long bucketIndexTo;

    private Long from;
    private Long to;

    // Filters
    private Map<String, String> filters = new HashMap<>();

    // Group
    private Set<String> groupDimensions;

    private final long sourceResolution;


    private Long numberOfBuckets;
    private Long resolution;

    private final TimeSeriesAggregationPipeline aggregationPipeline;

    protected TimeSeriesAggregationQuery(TimeSeriesAggregationPipeline aggregationPipeline, Function<Long, Long> indexProjectionFunction) {
        this.aggregationPipeline = aggregationPipeline;
        this.indexProjectionFunction = indexProjectionFunction;
        this.sourceResolution = aggregationPipeline.getResolution();
    }

    protected Long getBucketIndexFrom() {
        return bucketIndexFrom;
    }

    protected Long getBucketIndexTo() {
        return bucketIndexTo;
    }

    /**
     * Specifies a time range for the query
     *
     * @param from the beginning of the time range in ms (Epoch time)
     * @param to   the end of the time range (Epoch time)
     * @return the builder
     */
    public TimeSeriesAggregationQuery range(long from, long to) {
        this.from = from;
        this.to = to;
        this.bucketIndexFrom = indexProjectionFunction.apply(from);
        this.bucketIndexTo = indexProjectionFunction.apply(to);
        return this;
    }

    /**
     * Specifies the mandatory list of key-value attributes that the series should match
     *
     * @param filters the mandatory key-value attributes that the series should match
     * @return the builder
     */
    public TimeSeriesAggregationQuery filter(Map<String, String> filters) {
        this.filters.putAll(filters);
        return this;
    }

    /**
     * Group all the selected series in one series
     *
     * @return the builder
     */
    public TimeSeriesAggregationQuery group() {
        this.groupDimensions = Set.of();
        return this;
    }

    public TimeSeriesAggregationQuery groupBy(Set<String> dimensions) {
        this.groupDimensions = dimensions;
        return this;
    }

    public TimeSeriesAggregationQuery window(long intervalSizeMs) {
        if (intervalSizeMs < sourceResolution) {
            throw new IllegalArgumentException("The intervalSizeMs cannot be lower than the source resolution of " + sourceResolution + "ms");
        }
        this.resolution = intervalSizeMs;
        return this;
    }

    public TimeSeriesAggregationQuery split(long numberOfBuckets) {
        // TODO support split also when the range isn't specified
        if (bucketIndexTo == null || bucketIndexFrom == null) {
            throw new IllegalArgumentException("The method range() should be called before calling split()");
        }
        this.numberOfBuckets = numberOfBuckets;
        return this;
    }

    public Map<String, String> getFilters() {
        return filters;
    }

    public Set<String> getGroupDimensions() {
        return groupDimensions;
    }

    public long getResolution() {
        return resolution != null ? resolution : sourceResolution;
    }

    public Function<Long, Long> getIndexProjectionFunction() {
        if (resolution != null) {
            return aggregationPipeline.getIndexProjectionFunctionFactory().apply(resolution);
        } else if (numberOfBuckets != null) {
            long intervalSizeMs = (long) Math.ceil((double) (to - from) / numberOfBuckets);
            return index -> Math.max(from, index - (index - from) % intervalSizeMs);
        } else {
            return aggregationPipeline.getIndexProjectionFunctionFactory().apply(sourceResolution);
        }
    }

    public List<Long> drawAxis() {
        ArrayList<Long> legend = new ArrayList<>();
        if (from != null && to != null) {
            if (resolution != null) {
                for (long index = bucketIndexFrom; index < bucketIndexTo; index += resolution) {
                    legend.add(index);
                }
            } else if (numberOfBuckets != null) {
                long intervalSizeMs = (long) Math.ceil((double) (to - from) / numberOfBuckets);
                for (long index = from; index < to; index += intervalSizeMs) {
                    legend.add(index);
                }
            }
        }
        return legend;
    }

    public TimeSeriesAggregationResponse run() {
        return aggregationPipeline.collect(this);
    }
}

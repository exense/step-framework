package step.core.timeseries;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TimeSeriesAggregationQuery {

    // TODO make a constructor with this instead
    private static final int RESOLUTION = 1000;

    // Range
    private Long from;
    private Long to;

    // Filters
    private Map<String, String> filters = new HashMap<>();
    private boolean threadGroupsBuckets = false;

    // Group
    private Set<String> groupDimensions;

    private long intervalSizeMs = 1000L;

    private int numberOfBuckets;

    private final TimeSeriesAggregationPipeline aggregationPipeline;

    protected TimeSeriesAggregationQuery(TimeSeriesAggregationPipeline aggregationPipeline) {
        this.aggregationPipeline = aggregationPipeline;
    }

    public Long getFrom() {
        return from;
    }

    public Long getTo() {
        return to;
    }

    public boolean isThreadGroupsBuckets() {
        return threadGroupsBuckets;
    }

    public TimeSeriesAggregationQuery withThreadGroupsBuckets(boolean onlyThreadGroupsBuckets) {
        this.threadGroupsBuckets = onlyThreadGroupsBuckets;
        return this;
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
        if (from == null || to == null) {
            throw new IllegalArgumentException("window() function must be called when from and to are set");
        }
        if (intervalSizeMs <= 0) {
            return this;
        }
        this.intervalSizeMs = intervalSizeMs;
        return this;
    }

    public TimeSeriesAggregationQuery split(Long numberOfBuckets) {
        if (numberOfBuckets == null) {
            return this;
        }
        // TODO support split also when the range isn't specified
        if (to == null || from == null) {
            throw new IllegalArgumentException("The method range() should be called before calling split()");
        }
        long paddedFrom = from - from % RESOLUTION;
        long paddedTo = to + (RESOLUTION - (to % RESOLUTION));
        long intervalSizeMs = (paddedTo - paddedFrom) / numberOfBuckets;
        if (intervalSizeMs <= 0) {
            throw new IllegalArgumentException("Spitting into " + numberOfBuckets + " results in an interval size of "
                    + intervalSizeMs + "ms. The interval size should be higher than 0");
        }
        return window(intervalSizeMs);
    }

    public Map<String, String> getFilters() {
        return filters;
    }

    public Set<String> getGroupDimensions() {
        return groupDimensions;
    }

    public long getIntervalSizeMs() {
        return intervalSizeMs;
    }

    public TimeSeriesAggregationResponse run() {
        return aggregationPipeline.collect(this);
    }
}

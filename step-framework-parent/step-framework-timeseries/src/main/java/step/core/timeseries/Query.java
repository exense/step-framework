package step.core.timeseries;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Query {

    private final TimeSeriesPipeline timeSeriesPipeline;

    // Range
    protected Long from;
    protected Long to;

    // Filters
    protected Map<String, String> filters = new HashMap<>();

    // Group
    protected Set<String> groupDimensions;

    protected long intervalSizeMs = 1000L;

    public Query(TimeSeriesPipeline timeSeriesPipeline) {
        this.timeSeriesPipeline = timeSeriesPipeline;
    }

    /**
     * Specifies a time range for the query
     * @param from the beginning of the time range in ms (Epoch time)
     * @param to the end of the time range (Epoch time)
     * @return the builder
     */
    public Query range(long from, long to) {
        this.from = from;
        this.to = to;
        return this;
    }

    /**
     * Specifies the mandatory list of key-value attributes that the series should match
     * @param filters the mandatory key-value attributes that the series should match
     * @return the builder
     */
    public Query filter(Map<String, String> filters) {
        this.filters.putAll(filters);
        return this;
    }

    /**
     * Group all the selected series in one series
     * @return the builder
     */
    public Query group() {
        this.groupDimensions = Set.of();
        return this;
    }

    public Query groupBy(Set<String> dimensions) {
        this.groupDimensions = dimensions;
        return this;
    }

    public Query window(long intervalSizeMs) {
        this.intervalSizeMs = intervalSizeMs;
        return this;
    }

    public Query split(long numberOfBuckets) {
        // TODO support split also when the range isn't specified
        if (to == null || from == null) {
            throw new IllegalArgumentException("The method range() should be called before calling split()");
        }
        long intervalSizeMs = (to - from) / numberOfBuckets;
        if (intervalSizeMs <= 0) {
            throw new IllegalArgumentException("Spitting into " + numberOfBuckets + " results in an interval size of "
                    + intervalSizeMs + "ms. The interval size should be higher than 0");
        }
        return window(intervalSizeMs);
    }

    public Map<Map<String, String>, Map<Long, Bucket>> run() {
        return timeSeriesPipeline.runQuery(this);
    }

    public Map<Long, Bucket> runOne() {
        Map<Map<String, String>, Map<Long, Bucket>> result = timeSeriesPipeline.runQuery(this);
        return result.values().stream().findFirst().orElseThrow();
    }
}

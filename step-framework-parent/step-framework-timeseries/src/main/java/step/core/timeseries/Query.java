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

    protected long intervalSizeMs;

    public Query(TimeSeriesPipeline timeSeriesPipeline) {
        this.timeSeriesPipeline = timeSeriesPipeline;
    }

    public Query range(long from, long to) {
        this.from = from;
        this.to = to;
        return this;
    }

    public Query filter(Map<String, String> filters) {
        this.filters.putAll(filters);
        return this;
    }

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
        return window((to - from) / numberOfBuckets);
    }

    public Map<Map<String, String>, Map<Long, Bucket>> run() {
        return timeSeriesPipeline.runQuery(this);
    }

    public Map<Long, Bucket> runOne() {
        Map<Map<String, String>, Map<Long, Bucket>> result = timeSeriesPipeline.runQuery(this);
        return result.values().stream().findFirst().orElseThrow();
    }
}

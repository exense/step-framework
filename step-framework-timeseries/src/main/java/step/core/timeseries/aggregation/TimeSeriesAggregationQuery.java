package step.core.timeseries.aggregation;

import step.core.collections.Filter;
import step.core.timeseries.query.TimeSeriesQuery;

import java.util.*;
import java.util.function.Function;

public class TimeSeriesAggregationQuery extends TimeSeriesQuery {

    // Group
    private Set<String> groupDimensions;

    // The resolution of the source time series
//    private final long sourceResolution;

    // if all buckets should be shrunk in one single bucket
    private boolean shrink = false;

    // The resolution of the
    private final long resultResolution;

    private final TimeSeriesAggregationPipeline aggregationPipeline;
    private Set<String> collectAttributeKeys;
    private int collectAttributesValuesLimit;

    protected TimeSeriesAggregationQuery(TimeSeriesAggregationPipeline aggregationPipeline,
                                         Filter filter,
                                         Set<String> groupDimensions,
                                         Long from,
                                         Long to,
                                         long resultResolution,
                                         boolean shrink,
                                         Set<String> collectAttributeKeys, int collectAttributesValuesLimit) {
        super(from, to, filter);
        this.aggregationPipeline = aggregationPipeline;
        this.shrink = shrink;
        this.groupDimensions = groupDimensions;
        this.resultResolution = resultResolution;
        this.collectAttributeKeys = collectAttributeKeys;
        this.collectAttributesValuesLimit = collectAttributesValuesLimit;
    }

    public Set<String> getGroupDimensions() {
        return groupDimensions;
    }

    public List<Long> drawAxis() {
        ArrayList<Long> legend = new ArrayList<>();
        if (from != null && to != null) {
            if (shrink) {
                legend.add(from);
            } else {
                for (long index = from; index < to; index += resultResolution) {
                    legend.add(index);
                }
            }
        }
        return legend;
    }

    public Function<Long, Long> getProjectionFunction() {
        long start = Objects.requireNonNullElse(from, 0L);
        if (shrink) {
            return t -> start;
        } else {
            return t -> {
                long distanceFromStart = t - start;
                return distanceFromStart - distanceFromStart % resultResolution + start;
            };
        }
    }

    public long getBucketSize() {
        if (shrink) {
            if (from != null) {
                return to - from;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return resultResolution;
        }
    }

    public long getResolution() {
        return resultResolution;
    }

    public Filter getFilter() {
        return filter;
    }

    public Set<String> getCollectAttributeKeys() {
        return collectAttributeKeys;
    }

    public int getCollectAttributesValuesLimit() {
        return collectAttributesValuesLimit;
    }

    public TimeSeriesAggregationResponse run() {
        return aggregationPipeline.collect(this);
    }
}

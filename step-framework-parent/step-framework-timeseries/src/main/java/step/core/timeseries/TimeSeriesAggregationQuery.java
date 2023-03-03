package step.core.timeseries;

import step.core.collections.Filter;

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
    // The timestamp of the lower bound bucket to be included in the result
    private final Long resultFrom;
    // The timestamp of the upper bound bucket (exclusive)
    private final Long resultTo;

    private final TimeSeriesAggregationPipeline aggregationPipeline;

    public TimeSeriesAggregationQuery(TimeSeriesAggregationPipeline aggregationPipeline,
                                         Filter filter,
                                         Set<String> groupDimensions,
                                         Long from,
                                         Long to,
                                         Long resultFrom,
                                         Long resultTo,
                                         long resultResolution,
                                         boolean shrink
    ) {
        super(from, to, filter);
        this.aggregationPipeline = aggregationPipeline;
        this.shrink = shrink;
        this.groupDimensions = groupDimensions;
        this.resultFrom = resultFrom;
        this.resultTo = resultTo;
        this.resultResolution = resultResolution;
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



    protected Set<String> getGroupDimensions() {
        return groupDimensions;
    }

    protected Long getBucketIndexFrom() {
        return resultFrom;
    }

    protected Long getBucketIndexTo() {
        return resultTo;
    }

    protected List<Long> drawAxis() {
        ArrayList<Long> legend = new ArrayList<>();
        if (from != null && to != null) {
            if (shrink) {
                legend.add(resultFrom);
            } else {
                for (long index = resultFrom; index < resultTo; index += resultResolution) {
                    legend.add(index);
                }
            }
        }
        return legend;
    }

    protected Function<Long, Long> getProjectionFunction() {
        if (shrink) {
            return t -> Objects.requireNonNullElse(resultFrom, 0L);
        } else {
            return t -> TimeSeries.timestampToBucketTimestamp(t, resultResolution);
        }
    }

    protected long getBucketSize() {
        if (shrink) {
            if (resultFrom != null) {
                return resultTo - resultFrom;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return resultResolution;
        }
    }

    public Filter getFilter() {
        return filter;
    }

    public TimeSeriesAggregationResponse run() {
        return aggregationPipeline.collect(this);
    }
}

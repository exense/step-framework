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
    private long resultResolution;
    // The timestamp of the lower bound bucket to be included in the result
    private Long resultFrom;
    // The timestamp of the upper bound bucket (exclusive)
    private Long resultTo;

    private Filter filter;

    private final TimeSeriesAggregationPipeline aggregationPipeline;

    protected TimeSeriesAggregationQuery(TimeSeriesAggregationPipeline aggregationPipeline,
                                         Filter filter,
                                         Set<String> groupDimensions,
                                         Long from,
                                         Long to,
                                         Long resultFrom,
                                         Long resultTo,
                                         long resultResolution,
                                         boolean shrink
    ) {
        this.aggregationPipeline = aggregationPipeline;
        this.shrink = shrink;
//        this.sourceResolution = aggregationPipeline.getSourceResolution();
//        this.resultResolution = sourceResolution;
        this.filter = filter;
        this.groupDimensions = groupDimensions;
        this.from = from;
        this.to = to;
        this.resultFrom = resultFrom;
        this.resultTo = resultTo;
        this.resultResolution = resultResolution;
    }

    @Override
    public TimeSeriesAggregationQuery filter(String oqlFilter) {
        super.filter(oqlFilter);
        return this;
    }

    @Override
    public TimeSeriesAggregationQuery filter(Map<String, String> filters) {
        super.filter(filters);
        return this;
    }

    @Override
    public TimeSeriesAggregationQuery range(long from, long to) {
        super.range(from, to);
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
            if (resultFrom != null) {
                return t -> resultFrom;
            } else {
                return t -> 0L;
            }
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

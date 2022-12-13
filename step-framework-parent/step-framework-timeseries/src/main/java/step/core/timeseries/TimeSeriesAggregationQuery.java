package step.core.timeseries;

import java.util.*;
import java.util.function.Function;

public class TimeSeriesAggregationQuery extends TimeSeriesQuery {

    // Group
    private Set<String> groupDimensions;

    // The resolution of the source time series
    private final long sourceResolution;

    // if all buckets should be shrinked in one single bucket
    private boolean shrink = false;

    // The resolution of the
    private long resultResolution;
    // The timestamp of the lower bound bucket to be included in the result
    private Long resultFrom;
    // The timestamp of the upper bound bucket (exclusive)
    private Long resultTo;

    private final TimeSeriesAggregationPipeline aggregationPipeline;

    protected TimeSeriesAggregationQuery(TimeSeriesAggregationPipeline aggregationPipeline) {
        this.aggregationPipeline = aggregationPipeline;
        this.sourceResolution = aggregationPipeline.getSourceResolution();
        this.resultResolution = sourceResolution;
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

    /**
     * Defines the desired resolution. <br>
     * If the desired resolution isn't a multiple of the source resolution it will floored to the closest valid resolution
     *
     * @param resolution the desired resolution in ms
     * @return the builder
     */
    public TimeSeriesAggregationQuery window(long resolution) {
        if (resolution < sourceResolution) {
            throw new IllegalArgumentException("The resolution cannot be lower than the source resolution of " + sourceResolution + "ms");
        }
        this.resultResolution = Math.max(sourceResolution, resolution - resolution % sourceResolution);
        return this;
    }

    /**
     * Specifies the desired number of buckets in the result.
     * - For targetNumberOfBuckets = 1 the resulting number of buckets will be exactly 1
     * - For targetNumberOfBuckets > 1 the resulting number of buckets will be as close as possible to the specified target
     *
     * @param targetNumberOfBuckets the desired number of buckets in the result
     * @return the builder
     */
    public TimeSeriesAggregationQuery split(long targetNumberOfBuckets) {
        if (targetNumberOfBuckets == 1) {
            shrink = true;
            window(Long.MAX_VALUE);
        } else {
            if (from == null || to == null) {
                throw new IllegalArgumentException("No range specified. The method range() should be called before calling split()");
            }
            long targetResolution = (long) Math.ceil((double) (to - from) / targetNumberOfBuckets);
            long resolution = targetResolution - targetResolution % sourceResolution;
            this.resultResolution = Math.max(sourceResolution, resolution);
        }
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
            if(shrink) {
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
        if(shrink) {
            if(resultFrom != null) {
                return t -> resultFrom;
            } else {
                return t -> 0L;
            }
        } else {
            return t -> TimeSeries.timestampToBucketTimestamp(t, resultResolution);
        }
    }

    protected long getBucketSize() {
        if(shrink) {
            if(resultFrom != null) {
                return resultTo - resultFrom;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return resultResolution;
        }
    }

    public TimeSeriesAggregationResponse run() {
        if (from != null && to != null) {
            if(shrink) {
                resultFrom = from - from % sourceResolution;
                resultTo = to - to % sourceResolution + sourceResolution;
            } else {
                resultFrom = from - from % resultResolution;
                resultTo = to - (to - resultFrom) % resultResolution + resultResolution;
            }
        }

        return aggregationPipeline.collect(this);
    }
}

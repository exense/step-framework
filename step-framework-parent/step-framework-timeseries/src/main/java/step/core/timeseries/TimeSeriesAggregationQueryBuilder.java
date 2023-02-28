package step.core.timeseries;

import step.core.collections.Filter;

import java.util.HashSet;
import java.util.Set;

public class TimeSeriesAggregationQueryBuilder {

    private final TimeSeriesAggregationPipeline pipeline;
    private final long sourceResolution;
    private long resultResolution;
    private Set<String> groupDimensions = new HashSet<>();
    private Filter filter;
    private boolean shrink;
    private Long from;
    private Long to;


    public TimeSeriesAggregationQueryBuilder(TimeSeriesAggregationPipeline aggregationPipeline) {
        this.pipeline = aggregationPipeline;
        this.sourceResolution = aggregationPipeline.getSourceResolution();
        this.resultResolution = sourceResolution;
    }

    public TimeSeriesAggregationQueryBuilder withGroupDimensions(Set<String> groupDimensions) {
        this.groupDimensions = groupDimensions;
        return this;
    }

    public TimeSeriesAggregationQueryBuilder withFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public TimeSeriesAggregationQueryBuilder withShrink(boolean shrink) {
        this.shrink = shrink;
        return this;
    }

    public TimeSeriesAggregationQueryBuilder range(long from, long to) {
        this.from = from;
        this.to = to;
        return this;
    }

    /**
     * Defines the desired resolution. <br>
     * If the desired resolution isn't a multiple of the source resolution it will floored to the closest valid resolution
     *
     * @param resolution the desired resolution in ms
     * @return the builder
     */
    public TimeSeriesAggregationQueryBuilder window(long resolution) {
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
    public TimeSeriesAggregationQueryBuilder split(long targetNumberOfBuckets) {
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

    public TimeSeriesAggregationQuery build() {
        Long resultFrom = null;
        Long resultTo = null;
        if (from != null && to != null) {
            if (shrink) {
                resultFrom = from - from % sourceResolution;
                resultTo = to - to % sourceResolution + sourceResolution;
            } else {
                resultFrom = from - from % resultResolution;
                resultTo = to - (to - resultFrom) % resultResolution + resultResolution;
            }
        }

        return new TimeSeriesAggregationQuery(pipeline, filter, groupDimensions, from, to, resultFrom, resultTo, resultResolution, shrink);
    }


}

package step.core.timeseries.aggregation;

import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.HashSet;
import java.util.Set;

public class TimeSeriesAggregationQueryBuilder {

    private final TimeSeriesAggregationPipeline pipeline;
    private final long sourceResolution;
    private Set<String> groupDimensions = new HashSet<>();
    private Filter filter = Filters.empty();
    ;
    private Long from;
    private Long to;
    private boolean shrink;
    private Long proposedResolution;
    private Integer bucketsCount;


    public TimeSeriesAggregationQueryBuilder(TimeSeriesAggregationPipeline aggregationPipeline) {
        this.pipeline = aggregationPipeline;
        this.sourceResolution = aggregationPipeline.getSourceResolution();
    }

    public TimeSeriesAggregationQueryBuilder withGroupDimensions(Set<String> groupDimensions) {
        this.groupDimensions = groupDimensions;
        return this;
    }

    public TimeSeriesAggregationQueryBuilder withFilter(Filter filter) {
        this.filter = filter;
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
        this.proposedResolution = resolution;
        return this;
    }

    public TimeSeriesAggregationQueryBuilder split(int targetNumberOfBuckets) {
        if (targetNumberOfBuckets == 1) {
            shrink = true;
        } else {
            this.bucketsCount = targetNumberOfBuckets;
        }
        return this;
    }


    /**
     * The precedence of settings applied: shrink > bucketsCount > proposedResolution
     */
    public TimeSeriesAggregationQuery build() {
        Long resultFrom = null;
        Long resultTo = null;
        long resultResolution = sourceResolution;
        if (bucketsCount != null && (from == null || to == null)) {
            throw new IllegalArgumentException("While splitting, from and to params must be set");
        }
        if (from != null && to != null) {
            resultFrom = from - from % sourceResolution;
            resultTo = (long) Math.ceil((double) to / sourceResolution) * sourceResolution;
            if (shrink) { // we expand the interval to the closest completed resolutions
//                resultFrom = from - from % sourceResolution;
//                resultTo = to - to % sourceResolution + sourceResolution;
                resultResolution = Long.MAX_VALUE;
            } else {
                if (this.bucketsCount != null && this.bucketsCount > 0) {
                    resultFrom = from - from % sourceResolution;
                    resultTo = (long) Math.ceil((double) to / sourceResolution) * sourceResolution;
                    if ((resultTo - resultFrom) / sourceResolution <= this.bucketsCount) { // not enough buckets
//                        resultTo = resultFrom + this.bucketsCount * sourceResolution;
                        resultResolution = sourceResolution;
                    } else {
                        long difference = resultTo - resultFrom;
                        resultResolution = (long) Math.ceil(difference / (double) bucketsCount);
                        resultResolution = (long) Math.ceil((double) resultResolution / sourceResolution) * sourceResolution;
                        resultTo = resultFrom + (resultResolution * bucketsCount);

                    }
                } else if (this.proposedResolution != null && this.proposedResolution != 0) {
                    resultResolution = Math.max(sourceResolution, proposedResolution - proposedResolution % sourceResolution);
                    resultResolution = resultResolution - resultResolution % sourceResolution;
                    resultFrom = from - from % sourceResolution;
                    resultTo = (long) Math.ceil((double) to / resultResolution) * resultResolution;
                }
            }
        }

        return new TimeSeriesAggregationQuery(pipeline, filter, groupDimensions, resultFrom, resultTo, resultResolution, shrink);
    }


}

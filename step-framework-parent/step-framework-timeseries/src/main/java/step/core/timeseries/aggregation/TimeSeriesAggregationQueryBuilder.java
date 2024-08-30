package step.core.timeseries.aggregation;

import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.HashSet;
import java.util.Set;

public class TimeSeriesAggregationQueryBuilder {

    private Set<String> groupDimensions = new HashSet<>();
    private Filter filter = Filters.empty();
    private Long from;
    private Long to;
    private boolean shrink;
    private Long proposedResolution;
    private Integer bucketsCount;
    private Set<String> collectAttributeKeys;
    private int collectAttributesValuesLimit;

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
     * Optional: when providing a set of attribute keys to be collected, the aggregation pipeline will accumulate the unique values for each of these keys (per bucket).
     *
     * @param collectAttributeKeys the set of attributes keys which will be collected
     * @param collectAttributesValuesLimit the maximum unique values collected per attribute
     * @return the builder
     */
    public TimeSeriesAggregationQueryBuilder withAttributeCollection(Set<String> collectAttributeKeys, int collectAttributesValuesLimit) {
        this.collectAttributeKeys = collectAttributeKeys;
        this.collectAttributesValuesLimit = collectAttributesValuesLimit;
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
//        if (resolution < sourceResolution) {
//            throw new IllegalArgumentException("The resolution cannot be lower than the source resolution of " + sourceResolution + "ms");
//        }
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
        this.from = this.from != null ? this.from : 0;
//        Long resultFrom = null;
//        Long resultTo = null;
//        long resultResolution = sourceResolution;
//        if (bucketsCount != null && (from == null || to == null)) {
//            throw new IllegalArgumentException("While splitting, from and to params must be set");
//        }
//        if (from != null && to != null) {
//            resultFrom = roundDownToMultiple(from, sourceResolution);
//            resultTo = roundUpToMultiple(to, sourceResolution);
//            if (shrink) { // we expand the interval to the closest completed resolutions
//                resultResolution = Long.MAX_VALUE;
//            } else {
//                if (this.bucketsCount != null && this.bucketsCount > 0) {
//                    if ((resultTo - resultFrom) / sourceResolution <= this.bucketsCount) { // not enough buckets
//                        resultResolution = sourceResolution;
//                    } else {
//                        long difference = resultTo - resultFrom;
//                        resultResolution =  Math.round(difference / (double) bucketsCount);
//                        resultResolution =  Math.round((double) resultResolution / sourceResolution) * sourceResolution; // round to nearest multiple, up or down
//                    }
//                } else if (this.proposedResolution != null && this.proposedResolution != 0) {
//                    resultResolution = Math.max(sourceResolution, roundDownToMultiple(proposedResolution, sourceResolution));
//                    resultResolution = roundDownToMultiple(resultResolution, sourceResolution);
//                    resultTo = roundUpToMultiple(to, resultResolution);
//                }
//            }
//        }
        return new TimeSeriesAggregationQuery(filter, groupDimensions, this.proposedResolution, this.from, this.to, shrink, this.bucketsCount, collectAttributeKeys, collectAttributesValuesLimit);
    }

    private static long roundUpToMultiple(long value, long multiple) {
        return (long) Math.ceil((double) value / multiple) * multiple;
    }

    private static long roundDownToMultiple(long value, long multiple) {
        return value - value % multiple;
    }


}

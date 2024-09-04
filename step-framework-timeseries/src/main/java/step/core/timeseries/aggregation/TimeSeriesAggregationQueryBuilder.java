package step.core.timeseries.aggregation;

import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.timeseries.TimeSeriesFilterBuilder;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
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

    public TimeSeriesAggregationQueryBuilder withFilter(Map<String, Object> filter) {
        this.filter = TimeSeriesFilterBuilder.buildFilter(filter);
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
        return new TimeSeriesAggregationQuery(filter, groupDimensions, this.proposedResolution, this.from, this.to, shrink, this.bucketsCount, collectAttributeKeys, collectAttributesValuesLimit);
    }


}

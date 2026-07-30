package step.core.timeseries.aggregation;

import step.core.collections.Filter;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.query.TimeSeriesQuery;

import javax.annotation.Nullable;
import java.util.*;

public class TimeSeriesAggregationQuery extends TimeSeriesQuery {

    private TimeSeriesOptimizationType optimizationType;

    // Group
    private Set<String> groupDimensions;

    // Aggregation applied across the series of one group
    private Aggregation groupAggregation;

    // Aggregation applied across the successive buckets of one series falling into the same time window
    private Aggregation timeAggregation;

    // Ideal number of buckets the interval will be split into
    @Nullable
    private Integer bucketsCount;

    // custom resolution required. Optional. This resolution can be eventually rounded to match existing TS resolutions
    @Nullable
    private Long bucketsResolution;

    // if all buckets should be shrunk in one single bucket
    private boolean shrink = false;

    private Set<String> collectAttributeKeys;
    private int collectAttributesValuesLimit;

    protected TimeSeriesAggregationQuery(
        Filter filter,
        TimeSeriesOptimizationType optimizationType,
        Set<String> groupDimensions,
        Aggregation groupAggregation,
        Aggregation timeAggregation,
        Long bucketsResolution,
        Long from,
        Long to,
        boolean shrink,
        Integer bucketsCount,
        Set<String> collectAttributeKeys, int collectAttributesValuesLimit) {
        super(from, to, filter);
        this.shrink = shrink;
        this.optimizationType = optimizationType;
        this.bucketsCount = bucketsCount;
        this.groupDimensions = groupDimensions;
        this.groupAggregation = groupAggregation;
        this.timeAggregation = timeAggregation;
        this.bucketsResolution = bucketsResolution;
        this.collectAttributeKeys = collectAttributeKeys;
        this.collectAttributesValuesLimit = collectAttributesValuesLimit;
    }

    public Set<String> getGroupDimensions() {
        return groupDimensions;
    }

    public Aggregation getGroupAggregation() {
        return groupAggregation;
    }

    public Aggregation getTimeAggregation() {
        return timeAggregation;
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

    public boolean isShrink() {
        return shrink;
    }

    public TimeSeriesAggregationQuery setShrink(boolean shrink) {
        this.shrink = shrink;
        return this;
    }

    public Integer getBucketsCount() {
        return bucketsCount;
    }

    public TimeSeriesAggregationQuery setBucketsCount(Integer bucketsCount) {
        this.bucketsCount = bucketsCount;
        return this;
    }

    @Nullable
    public Long getBucketsResolution() {
        return bucketsResolution;
    }

    public TimeSeriesAggregationQuery setBucketsResolution(@Nullable Long bucketsResolution) {
        this.bucketsResolution = bucketsResolution;
        return this;
    }

    public TimeSeriesOptimizationType getOptimizationType() {
        return optimizationType;
    }
}

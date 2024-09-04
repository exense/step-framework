package step.core.timeseries.aggregation;

import step.core.collections.Filter;
import step.core.timeseries.query.TimeSeriesQuery;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public class TimeSeriesAggregationQuery extends TimeSeriesQuery {

    // Group
    private Set<String> groupDimensions;
    
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
                                         Set<String> groupDimensions,
                                         Long bucketsResolution,
                                         Long from,
                                         Long to,
                                         boolean shrink,
                                         Integer bucketsCount,
                                         Set<String> collectAttributeKeys, int collectAttributesValuesLimit) {
        super(from, to, filter);
        this.shrink = shrink;
        this.bucketsCount = bucketsCount;
        this.groupDimensions = groupDimensions;
        this.bucketsResolution = bucketsResolution;
        this.collectAttributeKeys = collectAttributeKeys;
        this.collectAttributesValuesLimit = collectAttributesValuesLimit;
    }

    public Set<String> getGroupDimensions() {
        return groupDimensions;
    }

//    public List<Long> drawAxis() {
//        ArrayList<Long> legend = new ArrayList<>();
//        if (from != null && to != null) {
//            if (shrink) {
//                legend.add(from);
//            } else {
//                for (long index = from; index < to; index += resultResolution) {
//                    legend.add(index);
//                }
//            }
//        }
//        return legend;
//    }
//
//    public Function<Long, Long> getProjectionFunction() {
//        long start = Objects.requireNonNullElse(from, 0L);
//        if (shrink) {
//            return t -> start;
//        } else {
//            return t -> {
//                long distanceFromStart = t - start;
//                return distanceFromStart - distanceFromStart % resultResolution + start;
//            };
//        }
//    }
//
//    public long getBucketSize() {
//        if (shrink) {
//            if (from != null) {
//                return to - from;
//            } else {
//                return Long.MAX_VALUE;
//            }
//        } else {
//            return resultResolution;
//        }
//    }
//
//    public long getResolution() {
//        return resultResolution;
//    }

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
}

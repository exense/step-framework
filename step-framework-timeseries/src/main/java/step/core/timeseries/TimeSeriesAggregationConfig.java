package step.core.timeseries;

public class TimeSeriesAggregationConfig {

    public static final int DEFAULT_RESPONSE_MAX_INTERVALS = 4000;
    public static final int DEFAULT_IDEAL_RESPONSE_INTERVALS = 100;
    public static final int DEFAULT_MAX_ALIGNMENT_INTERVALS = 500;

    /**
     * Optional limit representing the maximum intervals which cover the requested time range
     */
    private int responseMaxIntervals = DEFAULT_RESPONSE_MAX_INTERVALS;

    /**
     * Defines in how many equal intervals the requested range will be split. Use a higher value for more granular response.
     */
    private int idealResponseIntervals = DEFAULT_IDEAL_RESPONSE_INTERVALS;

    /**
     * Maximum number of alignment intervals the requested range may be split into. A scalar time aggregation is
     * applied on an alignment grid which is finer than the response resolution, and the aggregation pipeline holds
     * one builder per alignment interval and source series while collecting. This limit therefore bounds the memory
     * footprint of the aggregation: the finer the alignment grid, the more accurate the result, the more builders
     * are retained. When the limit doesn't allow for a grid finer than the response resolution, the aggregation
     * falls back to aligning on the response resolution itself.
     */
    private int maxAlignmentIntervals = DEFAULT_MAX_ALIGNMENT_INTERVALS;

    private boolean ttlEnabled;

    public int getResponseMaxIntervals() {
        return responseMaxIntervals;
    }

    public TimeSeriesAggregationConfig setResponseMaxIntervals(int responseMaxIntervals) {
        this.responseMaxIntervals = responseMaxIntervals;
        return this;
    }

    public int getIdealResponseIntervals() {
        return idealResponseIntervals;
    }

    public TimeSeriesAggregationConfig setIdealResponseIntervals(int idealResponseIntervals) {
        this.idealResponseIntervals = idealResponseIntervals;
        return this;
    }

    public int getMaxAlignmentIntervals() {
        return maxAlignmentIntervals;
    }

    public TimeSeriesAggregationConfig setMaxAlignmentIntervals(int maxAlignmentIntervals) {
        this.maxAlignmentIntervals = maxAlignmentIntervals;
        return this;
    }

    public boolean isTtlEnabled() {
        return ttlEnabled;
    }

    public TimeSeriesAggregationConfig setTtlEnabled(boolean ttlEnabled) {
        this.ttlEnabled = ttlEnabled;
        return this;
    }
}

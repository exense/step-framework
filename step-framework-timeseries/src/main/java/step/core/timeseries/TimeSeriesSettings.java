package step.core.timeseries;

public class TimeSeriesSettings {

    public static final int DEFAULT_RESPONSE_MAX_INTERVALS = 1000;
    public static final int DEFAULT_IDEAL_RESPONSE_INTERVALS = 100;

    /**
     * Optional limit representing the maximum intervals which cover the requested time range
     */
    private int responseMaxIntervals = DEFAULT_RESPONSE_MAX_INTERVALS;

    /**
     * Defines in how many equal intervals the requested range will be split. Use a higher value for more granular response.
     */
    private int idealResponseIntervals = DEFAULT_IDEAL_RESPONSE_INTERVALS;

    public int getResponseMaxIntervals() {
        return responseMaxIntervals;
    }

    public TimeSeriesSettings setResponseMaxIntervals(int responseMaxIntervals) {
        this.responseMaxIntervals = responseMaxIntervals;
        return this;
    }

    public int getIdealResponseIntervals() {
        return idealResponseIntervals;
    }

    public TimeSeriesSettings setIdealResponseIntervals(int idealResponseIntervals) {
        this.idealResponseIntervals = idealResponseIntervals;
        return this;
    }
}

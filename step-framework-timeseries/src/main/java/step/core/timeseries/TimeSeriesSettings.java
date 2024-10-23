package step.core.timeseries;

public class TimeSeriesSettings {

    /**
     * Optional limit representing the maximum intervals which cover the requested time range
     */
    private int responseMaxIntervals;

    public int getResponseMaxIntervals() {
        return responseMaxIntervals;
    }

    public TimeSeriesSettings setResponseMaxIntervals(int responseMaxIntervals) {
        this.responseMaxIntervals = responseMaxIntervals;
        return this;
    }
}

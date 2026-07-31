package step.core.timeseries.metric;

import jakarta.validation.constraints.NotNull;
import step.core.timeseries.bucket.Aggregation;

import java.util.Map;


public class MetricAggregation {
    @NotNull
    private MetricAggregationType type;

    private TwoStageAggregation twoStageAggregation;

    private Map<String, Object> params;

    public MetricAggregation() {
    }

    public MetricAggregation(MetricAggregationType type) {
        this.type = type;
    }

    public MetricAggregation(MetricAggregationType type, Map<String, Object> params) {
        this.type = type;
        this.params = params;
    }

    /**
     * Create a two-stage aggregation
     * @param timeAggregation how buckets of each series are aggregated over time.
     * @param groupAggregation how the series of the same group are aggregated.
     */
    public MetricAggregation(Aggregation timeAggregation, Aggregation groupAggregation) {
        this.type = MetricAggregationType.TWO_STAGE;
        this.twoStageAggregation = new TwoStageAggregation(timeAggregation, groupAggregation);
    }

    public MetricAggregationType getType() {
        return type;
    }

    public MetricAggregation setType(MetricAggregationType type) {
        this.type = type;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public MetricAggregation setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public TwoStageAggregation getTwoStageAggregation() {
        return twoStageAggregation;
    }

    public void setTwoStageAggregation(TwoStageAggregation twoStageAggregation) {
        this.twoStageAggregation = twoStageAggregation;
    }
}

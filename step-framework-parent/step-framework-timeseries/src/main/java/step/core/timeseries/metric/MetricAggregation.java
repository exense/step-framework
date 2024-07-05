package step.core.timeseries.metric;

import java.util.Map;



public class MetricAggregation {
    private MetricAggregationType type;
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
}

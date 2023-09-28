package step.core.timeseries.metric;

public class MetricAttribute {
    
    private String value;
    private String label;

    public String getValue() {
        return value;
    }

    public MetricAttribute setValue(String value) {
        this.value = value;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public MetricAttribute setLabel(String label) {
        this.label = label;
        return this;
    }
}

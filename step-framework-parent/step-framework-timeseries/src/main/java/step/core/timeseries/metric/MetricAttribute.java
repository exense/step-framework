package step.core.timeseries.metric;

public class MetricAttribute {
    
    private String name;
    private String displayName;

    public String getName() {
        return name;
    }

    public MetricAttribute setName(String value) {
        this.name = value;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public MetricAttribute setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }
}

package step.core.timeseries.metric;

import jakarta.validation.constraints.NotNull;

public class MetricAttribute {
    
    @NotNull
    private String name;
    @NotNull
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

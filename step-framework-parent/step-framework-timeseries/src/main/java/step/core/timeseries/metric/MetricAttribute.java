package step.core.timeseries.metric;

import jakarta.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MetricAttribute {
    
    @NotNull
    private String name;
    @NotNull
    private String displayName;
    @NotNull
    private MetricAttributeType type;
    @NotNull
    private Map<String, Object> metadata = new HashMap<>();

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

    public MetricAttributeType getType() {
        return type;
    }

    public MetricAttribute setType(MetricAttributeType type) {
        this.type = type;
        return this;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public MetricAttribute setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }
}

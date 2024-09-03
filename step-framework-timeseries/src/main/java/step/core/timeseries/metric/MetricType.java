package step.core.timeseries.metric;

import jakarta.validation.constraints.NotNull;
import step.core.accessors.AbstractIdentifiableObject;

import java.util.ArrayList;
import java.util.List;

public class MetricType extends AbstractIdentifiableObject {
    
    @NotNull
    private String name;
    @NotNull
    private String displayName;
    private String description;
    @NotNull
    private List<MetricAttribute> attributes = new ArrayList<>();
    private String unit;
    @NotNull
    private MetricAggregation defaultAggregation;
    @NotNull
    private List<String> defaultGroupingAttributes = new ArrayList<>();
    @NotNull
    private MetricRenderingSettings renderingSettings = new MetricRenderingSettings();

    public String getName() {
        return name;
    }

    public MetricType setName(String name) {
        this.name = name;
        return this;
    }

    public String getDisplayName() {
        return displayName;
    }

    public MetricType setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MetricType setDescription(String description) {
        this.description = description;
        return this;
    }

    public MetricType setUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public List<MetricAttribute> getAttributes() {
        return attributes;
    }

    public MetricType setAttributes(List<MetricAttribute> attributes) {
        this.attributes = attributes;
        return this;
    }

    public MetricRenderingSettings getRenderingSettings() {
        return renderingSettings;
    }

    public MetricType setRenderingSettings(MetricRenderingSettings renderingSettings) {
        this.renderingSettings = renderingSettings;
        return this;
    }

    public String getUnit() {
        return unit;
    }

    public MetricAggregation getDefaultAggregation() {
        return defaultAggregation;
    }

    public MetricType setDefaultAggregation(MetricAggregation defaultAggregation) {
        this.defaultAggregation = defaultAggregation;
        return this;
    }

    public List<String> getDefaultGroupingAttributes() {
        return defaultGroupingAttributes;
    }

    public MetricType setDefaultGroupingAttributes(List<String> defaultGroupingAttributes) {
        this.defaultGroupingAttributes = defaultGroupingAttributes;
        return this;
    }
    
}

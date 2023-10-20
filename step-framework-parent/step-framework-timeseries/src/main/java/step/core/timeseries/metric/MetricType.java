package step.core.timeseries.metric;

import step.core.accessors.AbstractIdentifiableObject;

import java.util.List;

public class MetricType extends AbstractIdentifiableObject {

    private String key;
    private String name;
    private String description;
    private List<MetricAttribute> attributes;
    private MetricUnit unit = MetricUnit.EMPTY;
    private MetricAggregation defaultAggregation;
    private List<String> defaultGroupingAttributes;
    private MetricRenderingSettings renderingSettings;

    public String getKey() {
        return key;
    }

    public MetricType setKey(String key) {
        this.key = key;
        return this;
    }

    public String getName() {
        return name;
    }

    public MetricType setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MetricType setDescription(String description) {
        this.description = description;
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

    public MetricUnit getUnit() {
        return unit;
    }

    public MetricType setUnit(MetricUnit unit) {
        this.unit = unit;
        return this;
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

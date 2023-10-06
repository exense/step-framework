package step.core.timeseries.metric;

import step.core.accessors.AbstractIdentifiableObject;

import java.util.List;

public class MetricType extends AbstractIdentifiableObject {

    private String key;
    private String name;
    private String description;
    private List<MetricAttribute> attributes;
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
}

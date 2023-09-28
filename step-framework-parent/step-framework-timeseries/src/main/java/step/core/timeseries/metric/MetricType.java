package step.core.timeseries.metric;

import step.core.accessors.AbstractIdentifiableObject;

import java.util.List;
import java.util.Map;

public class MetricType extends AbstractIdentifiableObject {
    
    private String name;
    private String label;
    private String unit;
    private List<MetricAttribute> attributes;
    private String groupingAttribute;
    private Map<String, String> seriesColors; // can set predefined colors for known series.

    public String getName() {
        return name;
    }

    public MetricType setName(String name) {
        this.name = name;
        return this;
    }

    public String getUnit() {
        return unit;
    }

    public MetricType setUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public MetricType setLabel(String label) {
        this.label = label;
        return this;
    }

    public List<MetricAttribute> getAttributes() {
        return attributes;
    }

    public MetricType setAttributes(List<MetricAttribute> attributes) {
        this.attributes = attributes;
        return this;
    }

    public String getGroupingAttribute() {
        return groupingAttribute;
    }

    public MetricType setGroupingAttribute(String groupingAttribute) {
        this.groupingAttribute = groupingAttribute;
        return this;
    }

    public Map<String, String> getSeriesColors() {
        return seriesColors;
    }

    public MetricType setSeriesColors(Map<String, String> seriesColors) {
        this.seriesColors = seriesColors;
        return this;
    }
}

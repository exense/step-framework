package step.core.timeseries.metric;

import step.core.accessors.AbstractIdentifiableObject;

public class MetricType extends AbstractIdentifiableObject {
    
    private String label;
    private String oqlQuery;
    private MetricTypeRenderSettings renderSettings; // for chart rendering

    public String getLabel() {
        return label;
    }

    public MetricType setLabel(String label) {
        this.label = label;
        return this;
    }

    public MetricTypeRenderSettings getRenderSettings() {
        return renderSettings;
    }

    public MetricType setRenderSettings(MetricTypeRenderSettings renderSettings) {
        this.renderSettings = renderSettings;
        return this;
    }

    public String getOqlQuery() {
        return oqlQuery;
    }

    public MetricType setOqlQuery(String oqlQuery) {
        this.oqlQuery = oqlQuery;
        return this;
    }
}

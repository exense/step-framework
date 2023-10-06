package step.core.timeseries.metric;

import java.util.List;
import java.util.Map;

public class MetricRenderingSettings {
    private MetricUnit unit = MetricUnit.EMPTY;
    private MetricAggregation defaultAggregation;
    private Map<String, String> seriesColors; // can set predefined colors for known series.
    private List<String> defaultGroupingAttributes;

    public MetricUnit getUnit() {
        return unit;
    }

    public MetricRenderingSettings setUnit(MetricUnit unit) {
        this.unit = unit;
        return this;
    }

    public MetricAggregation getDefaultAggregation() {
        return defaultAggregation;
    }

    public MetricRenderingSettings setDefaultAggregation(MetricAggregation defaultAggregation) {
        this.defaultAggregation = defaultAggregation;
        return this;
    }

    public Map<String, String> getSeriesColors() {
        return seriesColors;
    }

    public MetricRenderingSettings setSeriesColors(Map<String, String> seriesColors) {
        this.seriesColors = seriesColors;
        return this;
    }

    public List<String> getDefaultGroupingAttributes() {
        return defaultGroupingAttributes;
    }

    public MetricRenderingSettings setDefaultGroupingAttributes(List<String> defaultGroupingAttributes) {
        this.defaultGroupingAttributes = defaultGroupingAttributes;
        return this;
    }
}

package step.core.timeseries.metric;

import java.util.List;
import java.util.Map;

/**
 * This class contains all needed settings for rendering a visual chart.
 */
public class MetricTypeRenderSettings {
    
    private String groupingAttribute; // the default grouping
    private List<String> groupingOptions; // in case we will allow the user to change the grouping like Google does.
    private Map<String, String> seriesColors; // can set predefined colors for known series.
    private String yAxesUnit; // e.g. '/hour' or 'ms' displayed both on the tooltip and on the y axes

    public String getGroupingAttribute() {
        return groupingAttribute;
    }

    public MetricTypeRenderSettings setGroupingAttribute(String groupingAttribute) {
        this.groupingAttribute = groupingAttribute;
        return this;
    }

    public List<String> getGroupingOptions() {
        return groupingOptions;
    }

    public MetricTypeRenderSettings setGroupingOptions(List<String> groupingOptions) {
        this.groupingOptions = groupingOptions;
        return this;
    }

    public Map<String, String> getSeriesColors() {
        return seriesColors;
    }

    public MetricTypeRenderSettings setSeriesColors(Map<String, String> seriesColors) {
        this.seriesColors = seriesColors;
        return this;
    }

    public String getYAxesUnit() {
        return yAxesUnit;
    }

    public MetricTypeRenderSettings setYAxesUnit(String yAxesUnit) {
        this.yAxesUnit = yAxesUnit;
        return this;
    }
}

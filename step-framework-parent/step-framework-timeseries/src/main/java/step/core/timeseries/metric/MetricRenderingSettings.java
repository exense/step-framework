package step.core.timeseries.metric;

import java.util.Map;

public class MetricRenderingSettings {
    private Map<String, String> seriesColors; // can set predefined colors for known series.
    
    public Map<String, String> getSeriesColors() {
        return seriesColors;
    }

    public MetricRenderingSettings setSeriesColors(Map<String, String> seriesColors) {
        this.seriesColors = seriesColors;
        return this;
    }

}

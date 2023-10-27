package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.timeseries.metric.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MetricTypeTest {

    @Test
    public void testBaseModel() {
        String name = "metricName";
        String label = "metricLabel";
        String unit = "ms";
        MetricAggregation aggregation = MetricAggregation.COUNT;
        Map<String, String> seriesColors = Map.of();
        List<MetricAttribute> attributes = Arrays.asList(new MetricAttribute().setDisplayName("displayName").setName("name"));

        List<String> grouping = Arrays.asList("groupBy");
        MetricType metric = new MetricType()
                .setName(name)
                .setDisplayName(label)
                .setDescription("Custom description")
                .setAttributes(attributes)
                .setDefaultAggregation(aggregation)
                .setDefaultGroupingAttributes(grouping)
                .setUnit(unit)
                .setRenderingSettings(new MetricRenderingSettings()
                    .setSeriesColors(seriesColors)
                );
        Assert.assertEquals(name, metric.getName());
        Assert.assertEquals(label, metric.getDisplayName());
        Assert.assertEquals(unit, metric.getUnit());
        Assert.assertEquals(aggregation, metric.getDefaultAggregation());
        Assert.assertEquals(attributes, metric.getAttributes());
        Assert.assertEquals(attributes.get(0).getDisplayName(), metric.getAttributes().get(0).getDisplayName());
        Assert.assertEquals(attributes.get(0).getName(), metric.getAttributes().get(0).getName());
        Assert.assertEquals(grouping, metric.getDefaultGroupingAttributes());
        Assert.assertEquals(seriesColors, metric.getRenderingSettings().getSeriesColors());

    }

}

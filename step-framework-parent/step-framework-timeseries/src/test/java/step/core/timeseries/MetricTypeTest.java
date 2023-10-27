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
        List<MetricAttribute> attributes = Arrays.asList(new MetricAttribute().setLabel("label").setValue("value"));

        List<String> grouping = Arrays.asList("groupBy");
        MetricType metric = new MetricType()
                .setKey(name)
                .setName(label)
                .setDescription("Custom description")
                .setAttributes(attributes)
                .setDefaultAggregation(aggregation)
                .setDefaultGroupingAttributes(grouping)
                .setUnit(unit)
                .setRenderingSettings(new MetricRenderingSettings()
                    .setSeriesColors(seriesColors)
                );
        Assert.assertEquals(name, metric.getKey());
        Assert.assertEquals(label, metric.getName());
        Assert.assertEquals(unit, metric.getUnit());
        Assert.assertEquals(aggregation, metric.getDefaultAggregation());
        Assert.assertEquals(attributes, metric.getAttributes());
        Assert.assertEquals(attributes.get(0).getLabel(), metric.getAttributes().get(0).getLabel());
        Assert.assertEquals(attributes.get(0).getValue(), metric.getAttributes().get(0).getValue());
        Assert.assertEquals(grouping, metric.getDefaultGroupingAttributes());
        Assert.assertEquals(seriesColors, metric.getRenderingSettings().getSeriesColors());

    }

}

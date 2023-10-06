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
        MetricUnit unit = MetricUnit.MS;
        String grouping = "groupBy";
        MetricAggregation aggregation = MetricAggregation.COUNT;
        Map<String, String> seriesColors = Map.of();
        List<MetricAttribute> attributes = Arrays.asList(new MetricAttribute().setLabel("label").setValue("value"));

        MetricType metric = new MetricType()
                .setKey(name)
                .setName(label)
                .setDescription("Custom description")
                .setAttributes(attributes)
                .setRenderingSettings(new MetricRenderingSettings()
                    .setDefaultAggregation(aggregation)
                    .setDefaultGroupingAttributes(Arrays.asList(grouping))
                    .setUnit(unit)
                    .setSeriesColors(seriesColors)
                );
        Assert.assertEquals(name, metric.getKey());
        Assert.assertEquals(label, metric.getName());
        Assert.assertEquals(unit, metric.getRenderingSettings().getUnit());
        Assert.assertEquals(aggregation, metric.getRenderingSettings().getDefaultAggregation());
        Assert.assertEquals(attributes, metric.getAttributes());
        Assert.assertEquals(attributes.get(0).getLabel(), metric.getAttributes().get(0).getLabel());
        Assert.assertEquals(attributes.get(0).getValue(), metric.getAttributes().get(0).getValue());
        Assert.assertEquals(grouping, metric.getRenderingSettings().getDefaultGroupingAttributes());
        Assert.assertEquals(seriesColors, metric.getRenderingSettings().getSeriesColors());

    }

}

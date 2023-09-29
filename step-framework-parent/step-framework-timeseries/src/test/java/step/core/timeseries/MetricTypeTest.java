package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.timeseries.metric.MetricAggregation;
import step.core.timeseries.metric.MetricAttribute;
import step.core.timeseries.metric.MetricType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MetricTypeTest {

    @Test
    public void testBaseModel() {
        String name = "metricName";
        String label = "metricLabel";
        String unit = "/sec";
        String grouping = "groupBy";
        MetricAggregation aggregation = MetricAggregation.AVG;
        Map<String, String> seriesColors = Map.of();
        List<MetricAttribute> attributes = Arrays.asList(new MetricAttribute().setLabel("label").setValue("value"));

        MetricType metric = new MetricType()
                .setName(name)
                .setLabel(label)
                .setDefaultAggregation(aggregation)
                .setGroupingAttribute(grouping)
                .setAttributes(attributes)
                .setUnit(unit)
                .setSeriesColors(seriesColors);
        Assert.assertEquals(name, metric.getName());
        Assert.assertEquals(label, metric.getLabel());
        Assert.assertEquals(unit, metric.getUnit());
        Assert.assertEquals(aggregation, metric.getDefaultAggregation());
        Assert.assertEquals(attributes, metric.getAttributes());
        Assert.assertEquals(attributes.get(0).getLabel(), metric.getAttributes().get(0).getLabel());
        Assert.assertEquals(attributes.get(0).getValue(), metric.getAttributes().get(0).getValue());
        Assert.assertEquals(grouping, metric.getGroupingAttribute());
        Assert.assertEquals(seriesColors, metric.getSeriesColors());

    }

}

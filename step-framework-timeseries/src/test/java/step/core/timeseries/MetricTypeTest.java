package step.core.timeseries;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import step.core.accessors.DefaultJacksonMapperProvider;
import step.core.timeseries.bucket.Aggregation;
import step.core.timeseries.metric.*;

import java.util.*;

public class MetricTypeTest {

    @Test
    public void testMetricAggregationParams() {
        MetricAggregation aggregation = new MetricAggregation(MetricAggregationType.PERCENTILE, Map.of("pclValue", 80));
        MetricType metric = new MetricType()
            .setName("newMetric")
            .setDisplayName("label")
            .setDescription("Custom description")
            .setAttributes(Collections.emptyList())
            .setDefaultAggregation(aggregation)
            .setDefaultGroupingAttributes(Arrays.asList("name"))
            .setUnit("unit");
        Assert.assertEquals(80, metric.getDefaultAggregation().getParams().get("pclValue"));

    }

    @Test
    public void testBaseModel() {
        String name = "metricName";
        String label = "metricLabel";
        String unit = "ms";
        MetricAggregation aggregation = new MetricAggregation(MetricAggregationType.COUNT);
        Map<String, String> seriesColors = Map.of();
        List<MetricAttribute> attributes = Arrays.asList(
            new MetricAttribute()
                .setType(MetricAttributeType.TEXT)
                .setDisplayName("displayName")
                .setMetadata(Map.of("metadataKey", "metadataValue"))
                .setName("name"),
            new MetricAttribute()
                .setType(MetricAttributeType.NUMBER)
                .setDisplayName("Duration")
                .setName("duration"),
            new MetricAttribute()
                .setType(MetricAttributeType.DATE)
                .setDisplayName("Created on")
                .setName("begin")

        );

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
        Assert.assertEquals(attributes.get(0).getType(), MetricAttributeType.TEXT);
        Assert.assertEquals("metadataValue", attributes.get(0).getMetadata().get("metadataKey"));
        Assert.assertEquals(grouping, metric.getDefaultGroupingAttributes());
        Assert.assertEquals(seriesColors, metric.getRenderingSettings().getSeriesColors());
        // Metrics are event driven unless explicitly declared as sampled
        Assert.assertEquals(MetricSamplingMode.EVENT_DRIVEN, metric.getSamplingMode());
        Assert.assertEquals(MetricSamplingMode.SAMPLED,
            metric.setSamplingMode(MetricSamplingMode.SAMPLED).getSamplingMode());
    }

    /**
     * Metric types persisted before the introduction of {@link MetricSamplingMode} must keep the historical
     * behaviour, i.e. be deserialized as {@link MetricSamplingMode#EVENT_DRIVEN}.
     */
    @Test
    public void testSamplingModeDefaultOnLegacyDocument() throws JsonProcessingException {
        String legacyDocument = "{\"name\":\"metricName\",\"displayName\":\"metricLabel\",\"instrumentType\":\"gauge\"," +
            "\"unit\":\"1\",\"attributes\":[],\"defaultGroupingAttributes\":[],\"renderingSettings\":{}}";

        MetricType metric = DefaultJacksonMapperProvider.getObjectMapper().readValue(legacyDocument, MetricType.class);

        Assert.assertEquals(MetricSamplingMode.EVENT_DRIVEN, metric.getSamplingMode());
    }

    @Test
    public void testSamplingModeSerialization() throws JsonProcessingException {
        ObjectMapper objectMapper = DefaultJacksonMapperProvider.getObjectMapper();
        MetricType metric = new MetricType()
            .setName("metricName")
            .setDisplayName("metricLabel")
            .setInstrumentType("gauge")
            .setSamplingMode(MetricSamplingMode.SAMPLED);

        MetricType deserialized = objectMapper.readValue(objectMapper.writeValueAsString(metric), MetricType.class);

        Assert.assertEquals(MetricSamplingMode.SAMPLED, deserialized.getSamplingMode());
    }

    @Test
    public void testTwoPhaseAggregation() {
        MetricAggregation metricAggregation = new MetricAggregation(Aggregation.AVG, Aggregation.SUM);
        Assert.assertEquals(MetricAggregationType.TWO_STAGE, metricAggregation.getType());
        Assert.assertEquals(Aggregation.AVG, metricAggregation.getTwoStageAggregation().getTimeAggregation());
        Assert.assertEquals(Aggregation.SUM, metricAggregation.getTwoStageAggregation().getGroupAggregation());
    }
}

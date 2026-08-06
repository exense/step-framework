package step.core.timeseries.aggregation;

import step.core.collections.Filter;

import java.util.Set;

public class TimeSeriesProcessedParams {
    private long from;
    private long to;
    private long resolution;
    private boolean shrink;
    private Filter filter;
    private Set<String> groupDimensions;
    private Set<String> collectAttributeKeys;
    private int collectAttributesValuesLimit;
    private long samplingIntervalMs;

    /**
     * @return the interval in ms at which the queried series are sampled, 0 if unknown
     */
    public long getSamplingIntervalMs() {
        return samplingIntervalMs;
    }

    public TimeSeriesProcessedParams setSamplingIntervalMs(long samplingIntervalMs) {
        this.samplingIntervalMs = samplingIntervalMs;
        return this;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public long getResolution() {
        return resolution;
    }

    public Set<String> getGroupDimensions() {
        return groupDimensions;
    }

    public Filter getFilter() {
        return filter;
    }

    public boolean isShrink() {
        return shrink;
    }

    public TimeSeriesProcessedParams setShrink(boolean shrink) {
        this.shrink = shrink;
        return this;
    }

    public TimeSeriesProcessedParams setFrom(long from) {
        this.from = from;
        return this;
    }

    public TimeSeriesProcessedParams setTo(long to) {
        this.to = to;
        return this;
    }

    public TimeSeriesProcessedParams setResolution(long resolution) {
        this.resolution = resolution;
        return this;
    }

    public TimeSeriesProcessedParams setFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public TimeSeriesProcessedParams setGroupDimensions(Set<String> groupDimensions) {
        this.groupDimensions = groupDimensions;
        return this;
    }

    public Set<String> getCollectAttributeKeys() {
        return collectAttributeKeys;
    }

    public TimeSeriesProcessedParams setCollectAttributeKeys(Set<String> collectAttributeKeys) {
        this.collectAttributeKeys = collectAttributeKeys;
        return this;
    }

    public int getCollectAttributesValuesLimit() {
        return collectAttributesValuesLimit;
    }

    public TimeSeriesProcessedParams setCollectAttributesValuesLimit(int collectAttributesValuesLimit) {
        this.collectAttributesValuesLimit = collectAttributesValuesLimit;
        return this;
    }
}

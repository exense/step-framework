package step.core.timeseries.metric;

/**
 * Describes how the values of a metric are produced over time. This is a property of the data source, not of its
 * rendering: consumers such as charts derive from it whether the absence of a value carries any information.
 */
public enum MetricSamplingMode {

    /**
     * Values are only produced when something is observed (a keyword runs, a step ends, a custom gauge is set).
     * The absence of a value means that nothing was observed, not that the value changed, hence the last known
     * value remains valid until the next one arrives.
     */
    EVENT_DRIVEN,

    /**
     * Values are produced at a fixed interval as long as the series exists, either by a sampler or by a heartbeat
     * re-emitting the last known value. The absence of a value therefore means that the series itself is gone
     * (an agent left the grid, an execution ended) and the last known value must not be carried forward
     * indefinitely.
     */
    SAMPLED,
}
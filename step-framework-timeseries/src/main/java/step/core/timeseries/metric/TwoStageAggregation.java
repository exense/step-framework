package step.core.timeseries.metric;

import jakarta.validation.constraints.NotNull;
import step.core.timeseries.bucket.Aggregation;

import java.util.Objects;

/**
 * Aggregation of a metric performed in two explicit stages, as opposed to the single-stage {@link MetricAggregation}
 * which extracts one value out of the merged samples.
 * <p>
 * The first stage reduces the successive buckets of one series over time, the second one reduces the resulting values
 * of the series of one group. Both stages are independent and compose; see {@link Aggregation} for the semantics of
 * each combination.
 */
public class TwoStageAggregation {

    @NotNull
    private Aggregation timeAggregation;

    @NotNull
    private Aggregation groupAggregation;

    public TwoStageAggregation() {
    }

    public TwoStageAggregation(Aggregation timeAggregation, Aggregation groupAggregation) {
        this.timeAggregation = timeAggregation;
        this.groupAggregation = groupAggregation;
    }

    public Aggregation getTimeAggregation() {
        return timeAggregation;
    }

    public TwoStageAggregation setTimeAggregation(Aggregation timeAggregation) {
        this.timeAggregation = timeAggregation;
        return this;
    }

    public Aggregation getGroupAggregation() {
        return groupAggregation;
    }

    public TwoStageAggregation setGroupAggregation(Aggregation groupAggregation) {
        this.groupAggregation = groupAggregation;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TwoStageAggregation)) {
            return false;
        }
        TwoStageAggregation that = (TwoStageAggregation) o;
        return timeAggregation == that.timeAggregation && groupAggregation == that.groupAggregation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeAggregation, groupAggregation);
    }

    @Override
    public String toString() {
        return "TwoStageAggregation{timeAggregation=" + timeAggregation + ", groupAggregation=" + groupAggregation + '}';
    }
}

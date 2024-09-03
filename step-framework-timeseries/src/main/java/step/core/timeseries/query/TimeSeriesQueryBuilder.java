package step.core.timeseries.query;

import step.core.collections.Filter;
import step.core.collections.Filters;

public class TimeSeriesQueryBuilder {

    // Specified range
    private Long from;
    private Long to;

    private Filter filter = Filters.empty();

    public TimeSeriesQueryBuilder range(Long from, Long to) {
        this.from = from;
        this.to = to;
        return this;
    }

    public TimeSeriesQueryBuilder withFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public TimeSeriesQuery build() {
        return new TimeSeriesQuery(from, to, filter);
    }
}

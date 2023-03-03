package step.core.timeseries;

import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.timeseries.oql.OQLTimeSeriesFilterBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static step.core.timeseries.TimeSeriesConstants.ATTRIBUTES_PREFIX;
import static step.core.timeseries.TimeSeriesConstants.TIMESTAMP_ATTRIBUTE;

public class TimeSeriesFilterBuilder {

    public static Filter buildFilter(String oql) {
        return OQLTimeSeriesFilterBuilder.getFilter(oql);
    }

    public static Filter buildFilter(Long from, Long to) {
        ArrayList<Filter> filters = new ArrayList<>();
        if (from != null) {
            filters.add(Filters.gte(TIMESTAMP_ATTRIBUTE, from));
        }
        if (to != null) {
            filters.add(Filters.lt(TIMESTAMP_ATTRIBUTE, to));
        }
        return Filters.and(filters);
    }

    public static Filter buildFilter(Map<String, Object> attributes) {
        List<Filter> filters = new ArrayList<>();
        if (attributes != null) {
            filters = attributes.entrySet().stream()
                    .map(e -> {
                        Object value = e.getValue();
                        if (value instanceof Boolean) {
                            return Filters.equals(ATTRIBUTES_PREFIX + e.getKey(), (Boolean) e.getValue());
                        } else if (value instanceof Integer) {
                            return Filters.equals(ATTRIBUTES_PREFIX + e.getKey(), ((Integer) e.getValue()).longValue());
                        } else if (value instanceof Long) {
                            return Filters.equals(ATTRIBUTES_PREFIX + e.getKey(), (Long) e.getValue());
                        } else {
                            return Filters.equals(ATTRIBUTES_PREFIX + e.getKey(), e.getValue().toString());
                        }

                    }).collect(Collectors.toList());
        }
        return Filters.and(filters);
    }

    public static Filter buildFilter(TimeSeriesQuery query) {
        Filter timestampFilter = TimeSeriesFilterBuilder.buildFilter(query.getFrom(), query.getTo());

        return Filters.and(Arrays.asList(query.getFilter(), timestampFilter));
    }

    public static Filter buildFilter(TimeSeriesAggregationQuery query) {
        Filter timestampFilter = TimeSeriesFilterBuilder.buildFilter(query.getBucketIndexFrom(), query.getBucketIndexTo());

        return Filters.and(Arrays.asList(query.getFilter(), timestampFilter));
    }

}

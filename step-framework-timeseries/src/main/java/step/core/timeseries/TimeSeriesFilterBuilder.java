package step.core.timeseries;

import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesProcessedParams;
import step.core.timeseries.query.OQLTimeSeriesFilterBuilder;
import step.core.timeseries.query.TimeSeriesQuery;

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
        return (filters.isEmpty()) ? Filters.empty() : Filters.and(filters);
    }

    public static Filter buildFilter(Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Filters.empty();
        }
        List<Filter> filters = attributes.entrySet().stream()
                .map(e -> {
                    Object value = e.getValue();
                    String key = ATTRIBUTES_PREFIX + e.getKey();
                    if (value == null) {
                        return Filters.equals(key, (String) value);
                    } else if (value instanceof Boolean) {
                        return Filters.equals(key, (Boolean) value);
                    } else if (value instanceof Integer) {
                        return Filters.equals(key, ((Integer) value).longValue());
                    } else if (value instanceof Long) {
                        return Filters.equals(key, (Long) value);
                    } else {
                        return Filters.equals(key, value.toString());
                    }

                }).collect(Collectors.toList());
        return Filters.and(filters);
    }

    public static Filter buildFilter(TimeSeriesQuery query) {
        Filter timestampFilter = TimeSeriesFilterBuilder.buildFilter(query.getFrom(), query.getTo());

        return Filters.and(Arrays.asList(query.getFilter(), timestampFilter));
    }
    
    public static Filter buildFilter(TimeSeriesProcessedParams params) {
        Filter timestampFilter = TimeSeriesFilterBuilder.buildFilter(params.getFrom(), params.getTo());

        return Filters.and(Arrays.asList(params.getFilter(), timestampFilter));
    }

    public static Filter buildFilter(TimeSeriesAggregationQuery query) {
        Filter timestampFilter = TimeSeriesFilterBuilder.buildFilter(query.getFrom(), query.getTo());

        return Filters.and(Arrays.asList(query.getFilter(), timestampFilter));
    }

}

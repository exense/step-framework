package step.core.timeseries;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TimeSeries {

    private final Collection<Bucket> collection;
    private final Set<String> indexedFields;
    private final Integer timeSeriesResolution;

    public TimeSeries(Collection<Bucket> collection, Set<String> indexedAttributes, Integer timeSeriesResolution) {
        this.collection = collection;
        this.indexedFields = indexedAttributes;
        this.timeSeriesResolution = timeSeriesResolution;
        createIndexes();

    }

    public TimeSeries(CollectionFactory collectionFactory, String collectionName, Set<String> indexedAttributes, Integer ingestionResolutionPeriod) {
        this(collectionFactory.getCollection(collectionName, Bucket.class), indexedAttributes, ingestionResolutionPeriod);
    }

    private void createIndexes() {
        collection.createOrUpdateIndex("begin");
        indexedFields.forEach(f -> collection.createOrUpdateIndex("attributes."+f));
    }

    public void performHousekeeping(TimeSeriesQuery housekeepingQuery) {
        collection.remove(TimeSeries.buildFilter(housekeepingQuery.getFilters(), housekeepingQuery.getFrom(), housekeepingQuery.getTo()));
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline() {
        return new TimeSeriesIngestionPipeline(collection, timeSeriesResolution);
    }

    public TimeSeriesIngestionPipeline newIngestionPipeline(long flushingPeriodInMs) {
        return new TimeSeriesIngestionPipeline(collection, timeSeriesResolution, flushingPeriodInMs);
    }

    public TimeSeriesAggregationPipeline getAggregationPipeline() {
        return new TimeSeriesAggregationPipeline(collection, timeSeriesResolution);
    }

    protected static long timestampToBucketTimestamp(long timestamp, long resolution) {
        return timestamp - timestamp % resolution;
    }

    public static Filter buildFilter(Map<String, String> attributes, Long from, Long to) {
        ArrayList<Filter> filters = new ArrayList<>();
        if (from != null) {
            filters.add(Filters.gte("begin", from));
        }
        if (to != null) {
            filters.add(Filters.lt("begin", to));
        }

        if (attributes != null) {
            filters.addAll(attributes.entrySet().stream()
                    .map(e -> Filters.equals("attributes." + e.getKey(), e.getValue())).collect(Collectors.toList()));
        }
        return Filters.and(filters);
    }
}

package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.IndexField;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;
import step.core.timeseries.query.TimeSeriesQuery;
import step.core.timeseries.query.TimeSeriesQueryBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TimeSeriesCollection {
	
	private static final Logger logger = LoggerFactory.getLogger(TimeSeriesCollection.class);
	
	private final Collection<Bucket> collection;
	private final long resolution;
	private final TimeSeriesIngestionPipeline ingestionPipeline;
	private long ttl; // set to 0 in case deletion is never required

	public TimeSeriesCollection(Collection<Bucket> collection, long resolution) {
		this(collection, resolution, 0, new TimeSeriesIngestionPipeline(collection, resolution));
	}
	
	public TimeSeriesCollection(Collection<Bucket> collection, long resolution, long ttl) {
		this(collection, resolution, ttl, new TimeSeriesIngestionPipeline(collection, resolution));
	}
	
	public TimeSeriesCollection(Collection<Bucket> collection, long resolution, long ttl, TimeSeriesIngestionPipeline ingestionPipeline) {
		this.collection = collection;
		this.resolution = resolution;
		this.ttl = ttl;
		this.ingestionPipeline = ingestionPipeline;
	}

	public boolean isEmpty() {
		return getCollection().estimatedCount() == 0;
	}
	
	public void performHousekeeping() {
		if (ttl > 0) {
			long cleanupRangeStart = 0;
			long cleanupRangeEnd = System.currentTimeMillis();
			TimeSeriesQuery query = new TimeSeriesQueryBuilder().range(cleanupRangeStart, cleanupRangeEnd).build();
			Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
			this.collection.remove(filter);
			logger.debug("Housekeeping successfully performed for collection " + this.collection.getName());
		}
		
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		if (ttl < 0) {
			throw new IllegalArgumentException("Negative ttl value is not allowed");
		}
		this.ttl = ttl;
	}
	
	public void createIndexes(Set<IndexField> indexFields) {
        collection.createOrUpdateIndex("begin");
        Set<IndexField> renamedFieldIndexes = indexFields.stream().map(i -> new IndexField("attributes." + i.fieldName,
                i.order, i.fieldClass)).collect(Collectors.toSet());
        renamedFieldIndexes.forEach(collection::createOrUpdateIndex);
    }

	public Collection<Bucket> getCollection() {
		return collection;
	}

	public TimeSeriesIngestionPipeline getIngestionPipeline() {
		return ingestionPipeline;
	}

	public long getResolution() {
		return resolution;
	}
	
}

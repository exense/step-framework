package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.IndexField;
import step.core.timeseries.aggregation.TimeSeriesAggregationPipeline;
import step.core.timeseries.aggregation.TimeSeriesAggregationQuery;
import step.core.timeseries.aggregation.TimeSeriesAggregationResponse;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.bucket.BucketAttributes;
import step.core.timeseries.bucket.BucketBuilder;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

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

	public TimeSeriesCollection(Collection<Bucket> collection, long resolution) {
		this(collection, resolution, new TimeSeriesIngestionPipeline(collection, resolution));
	}
	
	public TimeSeriesCollection(Collection<Bucket> collection, long resolution, TimeSeriesIngestionPipeline ingestionPipeline) {
		this.collection = collection;
		this.resolution = resolution;
		this.ingestionPipeline = ingestionPipeline;
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
	
	public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
		this.ingestionPipeline.ingestPoint(attributes, timestamp, value);
	}
	
	public void ingestBucket(Bucket bucket) {
		this.ingestionPipeline.ingestBucket(bucket);
	}
	
	protected TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        Map<BucketAttributes, Map<Long, BucketBuilder>> seriesBuilder = new HashMap<>();

        Filter filter = TimeSeriesFilterBuilder.buildFilter(query);
        Function<Long, Long> projectionFunction = query.getProjectionFunction();
        LongAdder bucketCount = new LongAdder();
        long t1 = System.currentTimeMillis();
        try (Stream<Bucket> stream = collection.findLazy(filter, null, null, null, 0)) {
            stream.forEach(bucket -> {
                bucketCount.increment();
                BucketAttributes bucketAttributes = bucket.getAttributes();

                BucketAttributes seriesKey;
                if (CollectionUtils.isNotEmpty(query.getGroupDimensions())) {
                    seriesKey = bucketAttributes.subset(query.getGroupDimensions());
                } else {
                    seriesKey = new BucketAttributes();
                }
                Map<Long, BucketBuilder> series = seriesBuilder.computeIfAbsent(seriesKey, a -> new TreeMap<>());

                long index = projectionFunction.apply(bucket.getBegin());
                series.computeIfAbsent(index, i -> new BucketBuilder(i, i + query.getBucketSize()).withAccumulateAttributes(query.getCollectAttributeKeys(), query.getCollectAttributesValuesLimit())).accumulate(bucket);
            });
        }
        long t2 = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.info("Performed query in " + (t2 - t1) + "ms. Number of buckets processed: " + bucketCount.longValue());
        }

        Map<BucketAttributes, Map<Long, Bucket>> result = seriesBuilder.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
                e.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().build()))));
        return new TimeSeriesAggregationResponse(result, query.getBucketSize()).withAxis(query.drawAxis());
    }
}

package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import step.core.collections.CollectionFactory;
import step.core.timeseries.bucket.Bucket;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesBuilder {

    private final List<TimeSeriesCollection> handledCollections = new ArrayList<>();
    private TimeSeriesAggregationConfig aggregationConfig = new TimeSeriesAggregationConfig();


    public TimeSeriesBuilder registerCollections(List<TimeSeriesCollection> collections) {
        collections.forEach(this::registerCollection);
        return this;
    }

    public TimeSeriesBuilder registerCollection(TimeSeriesCollection collection) {
        handledCollections.add(collection);
        return this;
    }

    /**
     * Creates and registers all enabled collections from the given config.
     * Replaces the separate TimeSeriesCollectionsBuilder step.
     *
     * @param config                             multi-resolution configuration
     * @param collectionFactory                  factory used to create underlying collections
     * @param mainCollectionName                 name of the main (highest resolution) collection
     * @param ignoredAttributesForHighResolution attributes to omit from hour/day/week collections
     */
    public TimeSeriesBuilder withConfig(TimeSeriesConfig config, CollectionFactory collectionFactory,
                                        String mainCollectionName, Set<String> ignoredAttributesForHighResolution) {
        int flushSeriesQueueSize = config.getFlushSeriesQueueSize();
        int flushAsyncQueueSize = config.getFlushAsyncQueueSize();
        long flushOffsetMs = config.getFlushOffsetMs();
        addIfEnabled(true, mainCollectionName, Duration.ofMillis(config.getMainResolution()), config.getMainFlushInterval(),
            flushSeriesQueueSize, flushAsyncQueueSize, flushOffsetMs, null, collectionFactory);
        addIfEnabled(config.isPerMinuteEnabled(), mainCollectionName + "_minute", Duration.ofMinutes(1), config.getPerMinuteFlushInterval(),
            flushSeriesQueueSize, flushAsyncQueueSize, flushOffsetMs, null, collectionFactory);
        addIfEnabled(config.isHourlyEnabled(), mainCollectionName + "_hour", Duration.ofHours(1), config.getHourlyFlushInterval(),
            flushSeriesQueueSize, flushAsyncQueueSize, flushOffsetMs, ignoredAttributesForHighResolution, collectionFactory);
        addIfEnabled(config.isDailyEnabled(), mainCollectionName + "_day", Duration.ofDays(1), config.getDailyFlushInterval(),
            flushSeriesQueueSize, flushAsyncQueueSize, flushOffsetMs, ignoredAttributesForHighResolution, collectionFactory);
        addIfEnabled(config.isWeeklyEnabled(), mainCollectionName + "_week", Duration.ofDays(7), config.getWeeklyFlushInterval(),
            flushSeriesQueueSize, flushAsyncQueueSize, flushOffsetMs, ignoredAttributesForHighResolution, collectionFactory);
        return this;
    }

    private void addIfEnabled(boolean enabled, String collectionName, Duration resolution, long flushInterval,
                              int flushSeriesQueueSizeThreshold, int flushAsyncQueueSize, long flushOffsetMs,
                              Set<String> ignoredAttributes, CollectionFactory collectionFactory) {
        if (enabled) {
            TimeSeriesCollectionConfig settings = new TimeSeriesCollectionConfig()
                .setResolutionMs(resolution.toMillis())
                .setIngestionFlushingPeriodMs(flushInterval)
                .setIngestionFlushSeriesQueueSize(flushSeriesQueueSizeThreshold)
                .setIngestionFlushAsyncQueueSize(flushAsyncQueueSize)
                .setIngestionFlushOffsetMs(flushOffsetMs)
                .setIgnoredAttributes(ignoredAttributes);
            TimeSeriesCollection collection = new TimeSeriesCollection(collectionFactory.getCollection(collectionName, Bucket.class), settings);
            handledCollections.add(collection);
        }
    }

    /**
     * Each pipeline must have a resolution multiplier of the one before.
     */
    private void validateResolutions() {
        List<Long> sortedResolutions = handledCollections.stream()
            .map(TimeSeriesCollection::getResolutionMs)
            .sorted()
            .collect(Collectors.toList());
        for (int i = 1; i < sortedResolutions.size(); i++) {
            Long previousResolution = sortedResolutions.get(i - 1);
            Long currentResolution = sortedResolutions.get(i);
            if (Objects.equals(previousResolution, currentResolution) || currentResolution % previousResolution != 0) {
                throw new IllegalArgumentException(String.format("Current resolution %d is not multiple of the previous resolution %d", currentResolution, previousResolution));
            }
        }
    }

    private void validateCollectionsIgnoredAttributes() {
        for (int i = 0; i < handledCollections.size() - 1; i++) {
            Set<String> current = handledCollections.get(i).getIgnoredAttributes();
            Set<String> nextAttributes = handledCollections.get(i + 1).getIgnoredAttributes();

            if (CollectionUtils.isNotEmpty(current)) {
                if (nextAttributes == null || !nextAttributes.containsAll(current)) {
                    throw new IllegalArgumentException("Invalid ignored attributes for collection with index " + i);
                }
            }
        }
    }

    /**
     * Ordered by resolution, each ingestion pipeline will send his collected bucket to the next ingestion pipeline
     */
    private void linkIngestionPipelines() {

        // link ingestion pipelines so they behave like a chain
        for (int i = 0; i < handledCollections.size() - 1; i++) {
            TimeSeriesIngestionPipeline pipeline = handledCollections.get(i).getIngestionPipeline();
            TimeSeriesIngestionPipeline nextPipeline = handledCollections.get(i + 1).getIngestionPipeline();
            pipeline.setNextPipeline(nextPipeline);
        }
    }

    public TimeSeriesBuilder withAggregationConfig(TimeSeriesAggregationConfig aggregationConfig) {
        this.aggregationConfig = aggregationConfig;
        return this;
    }

    public TimeSeries build() {
        if (handledCollections.isEmpty()) {
            throw new IllegalArgumentException("At least one time series collection must be registered");
        }
        handledCollections.sort(Comparator.comparingLong(TimeSeriesCollection::getResolutionMs));
        validateResolutions();
        validateCollectionsIgnoredAttributes();
        linkIngestionPipelines();
        return new TimeSeries(handledCollections, aggregationConfig);
    }


}

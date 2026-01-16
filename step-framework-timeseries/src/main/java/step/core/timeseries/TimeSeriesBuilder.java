package step.core.timeseries;

import org.apache.commons.collections.CollectionUtils;
import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesBuilder {
	
	private final List<TimeSeriesCollection> handledCollections = new ArrayList<>();
	private TimeSeriesSettings settings = new TimeSeriesSettings();


	public TimeSeriesBuilder registerCollections(List<TimeSeriesCollection> collections) {
		collections.forEach(this::registerCollection);
		return this;
	}

	public TimeSeriesBuilder registerCollection(TimeSeriesCollection collection) {
		handledCollections.add(collection);
		return this;
	}
	
	/**
	 * Each pipeline must have a resolution multiplier of the one before.
	 */
	private void validateResolutions() {
		List<Long> sortedResolutions = handledCollections.stream()
				.map(TimeSeriesCollection::getResolution)
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

	public TimeSeriesBuilder setSettings(TimeSeriesSettings settings) {
		this.settings = settings;
		return this;
	}

	public TimeSeriesSettings getSettings() {
		return settings;
	}

	public TimeSeries build() {
		if (handledCollections.isEmpty()) {
			throw new IllegalArgumentException("At least one time series collection must be registered.");
		}
		handledCollections.sort(Comparator.comparingLong(TimeSeriesCollection::getResolution));
		validateResolutions();
		validateCollectionsIgnoredAttributes();
		linkIngestionPipelines();
		return new TimeSeries(handledCollections, settings);
	}


}

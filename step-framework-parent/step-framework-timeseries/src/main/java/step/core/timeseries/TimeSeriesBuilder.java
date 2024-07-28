package step.core.timeseries;

import step.core.timeseries.ingestion.TimeSeriesIngestionPipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TimeSeriesBuilder {
	
	private List<TimeSeriesCollection> handledCollections = new ArrayList<>();
	private Map<Long, TimeSeriesCollection> collectionsByResolution = new TreeMap<>();
	
	public TimeSeriesBuilder registerCollection(TimeSeriesCollection collection) {
		long resolution = collection.getResolution();
		if (collectionsByResolution.containsKey(resolution)) {
			throw new IllegalArgumentException("Resolution already present: " + resolution);
		}
		handledCollections.add(collection);
		collectionsByResolution.put(resolution, collection);
		validateResolutions();
		return this;
	}
	
	/**
	 * Each pipeline must have a resolution multiplier of the one before.
	 */
	private void validateResolutions() {
		List<Long> sortedResolutions = new ArrayList<>(collectionsByResolution.keySet());
		for (int i = 1; i < sortedResolutions.size(); i++) {
			Long previousResolution = sortedResolutions.get(i - 1);
			Long currentResolution = sortedResolutions.get(i);
			if (previousResolution <= currentResolution || currentResolution % previousResolution != 0) {
				throw new IllegalArgumentException("Invalid resolution: " + currentResolution);
			}
		}
	}

	/**
	 * Ordered by resolution, each ingestion pipeline will send his collected bucket to the next ingestion pipeline
	 */
	public void linkIngestionPipelines() {
		List<TimeSeriesCollection> pipelinesList = new ArrayList<>(collectionsByResolution.values());
		
		// link ingestion pipelines so they behave like a chain
		for (int i = 0; i < pipelinesList.size() - 1; i++) {
			TimeSeriesIngestionPipeline pipeline = pipelinesList.get(i).getIngestionPipeline();
			TimeSeriesIngestionPipeline nextPipeline = pipelinesList.get(i + 1).getIngestionPipeline();
			pipeline.setFlushCallback((nextPipeline::ingestBucket));
		}
	}
	
	public TimeSeries build() {
		if (collectionsByResolution.isEmpty()) {
			throw new IllegalArgumentException("At least one ingestion pipeline must be registered");
		}
		return new TimeSeries(new ArrayList<>(collectionsByResolution.values()));
	}
	
}

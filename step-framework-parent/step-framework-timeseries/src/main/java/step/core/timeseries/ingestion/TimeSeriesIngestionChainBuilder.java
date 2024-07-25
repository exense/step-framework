package step.core.timeseries.ingestion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TimeSeriesIngestionChainBuilder {
	
	private Map<Long, TimeSeriesIngestionPipeline> pipelinesByResolution = new TreeMap<>();
	
	public void registerPipeline(TimeSeriesIngestionPipeline pipeline) {
		pipelinesByResolution.put(pipeline.getResolution(), pipeline);
		validateResolutions();
	}
	
	/**
	 * Each pipeline must have a resolution multiplier of the one before.
	 */
	private void validateResolutions() {
		List<Long> sortedResolutions = new ArrayList<>(pipelinesByResolution.keySet());
		for (int i = 1; i < sortedResolutions.size(); i++) {
			Long previousResolution = sortedResolutions.get(i - 1);
			Long currentResolution = sortedResolutions.get(i);
			if (previousResolution <= currentResolution || currentResolution % previousResolution != 0) {
				throw new IllegalArgumentException("Invalid resolution: " + currentResolution);
			}
		}
	}
	
	public TimeSeriesIngestionChain build() {
		if (pipelinesByResolution.isEmpty()) {
			throw new IllegalArgumentException("At least one ingestion pipeline must be registered");
		}
		List<TimeSeriesIngestionPipeline> pipelinesList = new ArrayList<>(pipelinesByResolution.values());
		for (int i = 0; i < pipelinesByResolution.size() - 1; i++) {
			TimeSeriesIngestionPipeline pipeline = pipelinesList.get(i);
			TimeSeriesIngestionPipeline nextPipeline = pipelinesList.get(i + 1);
			pipeline.setFlushCallback((nextPipeline::ingestBucket));
		}
		
		return new TimeSeriesIngestionChain(pipelinesList);
	}
	
}

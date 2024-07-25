package step.core.timeseries.ingestion;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesIngestionChain {
	
	private final List<TimeSeriesIngestionPipeline> pipelines;
	
	TimeSeriesIngestionChain(List<TimeSeriesIngestionPipeline> pipelines) {
		this.pipelines = pipelines;
	}
	
	public void ingestPoint(Map<String, Object> attributes, long timestamp, long value) {
		pipelines.get(0).ingestPoint(attributes, timestamp, value);
	}
	
	public List<Long> getAvailableResolutions() {
		return pipelines.stream().map(TimeSeriesIngestionPipeline::getResolution).collect(Collectors.toList());
	}

	
	
}

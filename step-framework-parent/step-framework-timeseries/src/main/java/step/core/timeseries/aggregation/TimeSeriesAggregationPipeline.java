package step.core.timeseries.aggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.timeseries.TimeSeriesCollection;
import java.util.*;


public class TimeSeriesAggregationPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesAggregationPipeline.class);
    private Map<Long, TimeSeriesCollection> collectionsByResolution = new TreeMap<>();

    private final List<TimeSeriesCollection> collections;

    public TimeSeriesAggregationPipeline(List<TimeSeriesCollection> collections) {
        this.collections = collections;
        collections.forEach(collection -> collectionsByResolution.put(collection.getResolution(), collection));
    }
    
    private long roundRequiredResolution(long targetResolution) {
        List<Long> availableResolutions = getAvailableResolutions();
        for (int i = 1; i < availableResolutions.size(); i++) {
            if (availableResolutions.get(i) > targetResolution) {
                return availableResolutions.get(i - 1);
            }
        }
        return availableResolutions.get(availableResolutions.size() - 1); // last resolution
    }
    

    public TimeSeriesAggregationResponse collect(TimeSeriesAggregationQuery query) {
        
    }
    
    public List<Long> getAvailableResolutions() {
		return new ArrayList<>(collectionsByResolution.keySet());
	}
}

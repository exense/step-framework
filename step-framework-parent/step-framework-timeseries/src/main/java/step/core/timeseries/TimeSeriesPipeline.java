package step.core.timeseries;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TimeSeriesPipeline {

    private final BucketAccessor bucketAccessor;

    public TimeSeriesPipeline(BucketAccessor bucketAccessor) {
        this.bucketAccessor = bucketAccessor;
    }

    public Map<Long, Bucket> query(Map<String, String> criteria, long from, long to, long resolutionMs) {
        long t1 = System.currentTimeMillis();
        long duration = to - from;
        long nIntervals = duration / resolutionMs;
        Map<Long, BucketBuilder> result = new ConcurrentHashMap<>();
        bucketAccessor.findManyByCriteria(criteria).parallel().forEach(bucket -> {
            long index = (long) (1.0 * nIntervals * (bucket.getBegin() - from)) / duration;
            result.computeIfAbsent(index, k -> BucketBuilder.create()).accumulate(bucket);
        });
        long t2 = System.currentTimeMillis();
        System.out.println("Performed query in " + (t2 - t1) + "ms");
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }
}

package step.core.timeseries.migration;

import step.core.timeseries.Bucket;
import step.core.timeseries.BucketAttributes;
import step.core.timeseries.BucketBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class MigrationIngestPipeline {
    // attributes, time, bucketBuilder
    Map<BucketAttributes, Map<Long, BucketBuilder>> buckets = new HashMap<>();

    private final long resolution;

    public MigrationIngestPipeline(long resolutionInMs) {
        this.resolution = resolutionInMs;
    }

    public void ingest(long begin, long value, Map<String, Object> measurement) {
        measurement.remove("_id");
        BucketAttributes attributes = new BucketAttributes(measurement);
        long beginAnchor = begin - begin % resolution;
        buckets.computeIfAbsent(attributes, (k) -> new HashMap<>()).computeIfAbsent(beginAnchor, x -> new BucketBuilder(begin).withAttributes(attributes))
                .ingest(value);
    }

    public Stream<Bucket> getAllBuckets() {
        return this.buckets.values()
                .stream()
                .flatMap(m -> m.values().stream().map(b -> b.build()));

    }


}

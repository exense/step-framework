package step.core.timeseries.accessor;

import step.core.accessors.Accessor;
import step.core.timeseries.Bucket;
import step.core.timeseries.Query;

import java.util.Map;

public interface BucketAccessor extends Accessor<Bucket> {
    public static final String ENTITY_NAME = "buckets";

    Map<Map<String, Object>, Map<Long, Bucket>> collectBuckets(Query query);
}

package step.core.timeseries;

import step.core.accessors.AbstractAccessor;
import step.core.collections.Collection;
import step.core.timeseries.*;


class BucketAccessor extends AbstractAccessor<Bucket> {

    public static final String ENTITY_NAME = "buckets";

    public BucketAccessor(Collection<Bucket> collectionDriver) {
        super(collectionDriver);
        this.createOrUpdateIndex("attributes.$**");
        this.createOrUpdateIndex("begin");
    }

}

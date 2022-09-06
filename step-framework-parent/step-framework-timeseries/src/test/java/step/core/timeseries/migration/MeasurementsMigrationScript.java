package step.core.timeseries.migration;

import org.junit.BeforeClass;
import org.junit.Test;
import step.core.collections.*;
import step.core.collections.filters.Equals;
import step.core.collections.mongodb.MongoDBCollectionFactory;
import step.core.timeseries.Bucket;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class MeasurementsMigrationScript {

    private static Collection<Map> measurementCollection;
    private static Collection<Bucket> bucketCollection;

    @BeforeClass
    public static void setup() {
        Properties properties = new Properties();
        properties.put("host", "localhost");
        properties.put("database", "step-os");
        CollectionFactory collectionFactory = new MongoDBCollectionFactory(properties);
        bucketCollection = collectionFactory.getCollection("buckets2", Bucket.class);
        measurementCollection = collectionFactory.getCollection("measurements", Map.class);
    }

    @Test
    public void migrate() {
        String executionId = "62c42d6768f64952ffbb64a7";
        boolean cleanupBefore = true;
        Equals eIdFilter = Filters.equals("attributes.eId", executionId);
        Optional<Bucket> foundBucket = bucketCollection.find(eIdFilter, null, null, 1, 0).findFirst();
        if (foundBucket.isPresent()) {
            if (cleanupBefore) {
                bucketCollection.remove(eIdFilter);
            } else {
                throw new RuntimeException("Buckets for this execution already exists");
            }
        }
        System.out.println("Starting the migration ==============");

//        Filters.Equals threadGroupTypeFilter = Filters.equals("type", "threadgroup");
//        Filter measurementsFilter = Filters.and(Arrays.asList(Filters.not(threadGroupTypeFilter), Filters.equals("eId", executionId)));
        Filter measurementsFilter = Filters.equals("eId", executionId);
        MigrationIngestPipeline ingestPipeline = new MigrationIngestPipeline(1000);
        measurementCollection.find(measurementsFilter, new SearchOrder("begin", 1), null, null, 0).forEach(measurement -> {
            Long begin = (Long) measurement.get("begin");
            Long value = (Long) measurement.get("value");
            measurement.remove("rnId");
            measurement.remove("origin");
            measurement.remove("planId");
            measurement.remove("agentUrl");
            measurement.remove("id");
            measurement.remove("begin");
            measurement.remove("value");
            ingestPipeline.ingest(begin, value, measurement);
        });
        ingestPipeline.getAllBuckets()
                .sorted((b1, b2) -> (int) (b1.getBegin() - b2.getBegin()))
                .forEach(bucketCollection::save);


        System.out.println("Migration complete ==============");
    }


}

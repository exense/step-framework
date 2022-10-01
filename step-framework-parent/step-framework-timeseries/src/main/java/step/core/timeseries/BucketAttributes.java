package step.core.timeseries;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BucketAttributes extends HashMap<String, String> {

    public BucketAttributes() {
        super();
    }

    public BucketAttributes(Map<String, String> map) {
        super(map);
    }

    public BucketAttributes subset(Set<String> keys) {
        BucketAttributes subset = new BucketAttributes();
        this.keySet().stream().filter(keys::contains).forEach(k -> {
            subset.put(k ,get(k));
        });
        return subset;
    }

}

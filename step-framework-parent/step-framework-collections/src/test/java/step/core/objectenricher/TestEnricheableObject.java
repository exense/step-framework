package step.core.objectenricher;

import java.util.HashMap;
import java.util.Map;

public class TestEnricheableObject implements EnricheableObject {

    private Map<String, String> attributes = new HashMap<>();

    @Override
    public void addAttribute(String key, String value) {
        attributes.put(key, value);
    }

    @Override
    public String getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttributes(Map<String, String> attributes) {

    }
}

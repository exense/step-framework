package step.core.objectenricher;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ObjectEnricherComposerTest {

    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";

    @Test
    public void compose() {
        ObjectEnricher objectEnricher = ObjectEnricherComposer.compose(List.of(new ObjectEnricher() {
            @Override
            public Map<String, String> getAdditionalAttributes() {
                return Map.of(KEY_1, VALUE_1);
            }

            @Override
            public void accept(EnricheableObject enricheableObject) {
                enricheableObject.addAttribute(KEY_1, VALUE_1);
            }
        }, new ObjectEnricher() {
            @Override
            public Map<String, String> getAdditionalAttributes() {
                return Map.of(KEY_2, VALUE_2);
            }

            @Override
            public void accept(EnricheableObject enricheableObject) {
                enricheableObject.addAttribute(KEY_2, VALUE_2);
            }
        }));

        TestEnricheableObject enricheableObject = new TestEnricheableObject();
        objectEnricher.accept(enricheableObject);
        assertEquals(VALUE_1, enricheableObject.getAttribute(KEY_1));
        assertEquals(VALUE_2, enricheableObject.getAttribute(KEY_2));

        Map<String, String> additionalAttributes = objectEnricher.getAdditionalAttributes();
        assertEquals(Map.of(KEY_1, VALUE_1, KEY_2, VALUE_2), additionalAttributes);
    }

}
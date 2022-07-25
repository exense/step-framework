package step.core.objectenricher;

import org.junit.Test;
import step.core.AbstractContext;

import java.util.Map;

import static org.junit.Assert.*;

public class ObjectHookRegistryTest {

    @Test
    public void test() throws Exception {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new MyObjectHook());
        ObjectFilter objectFilter = objectHookRegistry.getObjectFilter(null);
        String oqlFilter = objectFilter.getOQLFilter();
        assertEquals("attributes.att1 = val1", oqlFilter);
        TestEnricheableObject t = new TestEnricheableObject();
        objectHookRegistry.getObjectEnricher(null).accept(t);
        assertEquals("val1", t.getAttribute("att1"));
        objectHookRegistry.rebuildContext(null, null);

        AbstractContext context = new AbstractContext() {};
        objectHookRegistry.rebuildContext(context, null);
        assertEquals("val1", context.get("att1"));

        assertTrue(objectHookRegistry.isObjectAcceptableInContext(null, null));

       assertTrue(objectHookRegistry.getObjectPredicate(null).test(t));
    }

    private static class MyObjectHook implements ObjectHook {

        @Override
        public ObjectFilter getObjectFilter(AbstractContext context) {
            return () -> "attributes.att1 = val1";
        }

        @Override
        public ObjectEnricher getObjectEnricher(AbstractContext context) {
            return new ObjectEnricher() {
                @Override
                public Map<String, String> getAdditionalAttributes() {
                    return null;
                }

                @Override
                public void accept(EnricheableObject enricheableObject) {
                    enricheableObject.addAttribute("att1", "val1");
                }
            };
        }

        @Override
        public void rebuildContext(AbstractContext context, EnricheableObject object) {
            if (context != null) {
                context.put("att1", "val1");
            }
        }

        @Override
        public boolean isObjectAcceptableInContext(AbstractContext context, EnricheableObject object) {
            return true;
        }
    }
}
package step.core.objectenricher;

import org.junit.Test;
import step.core.AbstractContext;

import java.util.Map;
import java.util.TreeMap;
import java.util.List;

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

    @Test
    public void testCheckObjectAccessNoViolations() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new MyObjectHook());
        
        TestEnricheableObject object = new TestEnricheableObject();
        ObjectAccessException exception = objectHookRegistry.checkObjectAccess(null, object);
        
        assertNull("No violations should return null", exception);
    }

    @Test
    public void testCheckObjectAccessSingleViolation() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new RestrictiveObjectHook("TenantHook", "WRONG_TENANT", "Object belongs to different tenant"));
        
        TestEnricheableObject object = new TestEnricheableObject();
        ObjectAccessException exception = objectHookRegistry.checkObjectAccess(null, object);
        
        assertNotNull("Should return exception with violations", exception);
        assertEquals("Should have single violation", 1, exception.getViolations().size());
        
        ObjectAccessViolation violation = exception.getViolations().get(0);
        assertEquals("TenantHook", violation.hookId);
        assertEquals("WRONG_TENANT", violation.errorCode);
        assertEquals("Object belongs to different tenant", violation.message);
        
        assertEquals("Object belongs to different tenant", exception.getMessage());
    }

    @Test
    public void testCheckObjectAccessMultipleViolations() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new RestrictiveObjectHook("TenantHook", "WRONG_TENANT", "Object belongs to different tenant"));
        objectHookRegistry.add(new RestrictiveObjectHook("PermissionHook", "INSUFFICIENT_PERMISSIONS", "User lacks required permissions"));
        
        TestEnricheableObject object = new TestEnricheableObject();
        ObjectAccessException exception = objectHookRegistry.checkObjectAccess(null, object);
        
        assertNotNull("Should return exception with violations", exception);
        assertEquals("Should have two violations", 2, exception.getViolations().size());
        
        assertEquals("Access denied by 2 access control rule(s)", exception.getMessage());
        
        List<ObjectAccessViolation> violations = exception.getViolations();
        assertEquals("TenantHook", violations.get(0).hookId);
        assertEquals("WRONG_TENANT", violations.get(0).errorCode);
        assertEquals("PermissionHook", violations.get(1).hookId);
        assertEquals("INSUFFICIENT_PERMISSIONS", violations.get(1).errorCode);
    }

    @Test
    public void testCheckObjectAccessWithDetails() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new DetailedRestrictiveObjectHook());
        
        TestEnricheableObject object = new TestEnricheableObject();
        ObjectAccessException exception = objectHookRegistry.checkObjectAccess(null, object);
        
        assertNotNull("Should return exception with violations", exception);
        assertEquals("Should have single violation", 1, exception.getViolations().size());
        
        ObjectAccessViolation violation = exception.getViolations().get(0);
        assertEquals("DetailedHook", violation.hookId);
        assertEquals("CUSTOM_ERROR", violation.errorCode);
        assertEquals("Custom error with details", violation.message);
        assertTrue(violation instanceof DetailedRestrictiveObjectHook.CustomObjectAccessViolation);
        assertEquals("value1", ((DetailedRestrictiveObjectHook.CustomObjectAccessViolation) violation).details.get("key1"));
        assertEquals("value2", ((DetailedRestrictiveObjectHook.CustomObjectAccessViolation) violation).details.get("key2"));
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
                public TreeMap<String, String> getAdditionalAttributes() {
                    return new TreeMap<>(Map.of("att1", "val1"));
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

    private static class RestrictiveObjectHook implements ObjectHook {
        private final String hookId;
        private final String errorCode;
        private final String message;

        public RestrictiveObjectHook(String hookId, String errorCode, String message) {
            this.hookId = hookId;
            this.errorCode = errorCode;
            this.message = message;
        }

        @Override
        public ObjectFilter getObjectFilter(AbstractContext context) {
            return () -> "";
        }

        @Override
        public ObjectEnricher getObjectEnricher(AbstractContext context) {
            return new ObjectEnricher() {
                @Override
                public TreeMap<String, String> getAdditionalAttributes() {
                    return new TreeMap<>();
                }

                @Override
                public void accept(EnricheableObject enricheableObject) {
                    // No-op
                }
            };
        }

        @Override
        public void rebuildContext(AbstractContext context, EnricheableObject object) {
            // No-op
        }

        @Override
        public boolean isObjectAcceptableInContext(AbstractContext context, EnricheableObject object) {
            return false;
        }

        @Override
        public String getHookIdentifier() {
            return hookId;
        }

        @Override
        public ObjectAccessViolation checkObjectAccess(AbstractContext context, EnricheableObject object) {
            return new ObjectAccessViolation(hookId, errorCode, message);
        }
    }

    private static class DetailedRestrictiveObjectHook implements ObjectHook {
        @Override
        public ObjectFilter getObjectFilter(AbstractContext context) {
            return () -> "";
        }

        @Override
        public ObjectEnricher getObjectEnricher(AbstractContext context) {
            return new ObjectEnricher() {
                @Override
                public TreeMap<String, String> getAdditionalAttributes() {
                    return new TreeMap<>();
                }

                @Override
                public void accept(EnricheableObject enricheableObject) {
                    // No-op
                }
            };
        }

        @Override
        public void rebuildContext(AbstractContext context, EnricheableObject object) {
            // No-op
        }

        @Override
        public boolean isObjectAcceptableInContext(AbstractContext context, EnricheableObject object) {
            return false;
        }

        @Override
        public String getHookIdentifier() {
            return "DetailedHook";
        }

        @Override
        public ObjectAccessViolation checkObjectAccess(AbstractContext context, EnricheableObject object) {
            Map<String, Object> details = Map.of(
                "key1", "value1",
                "key2", "value2"
            );
            return new CustomObjectAccessViolation("DetailedHook", "CUSTOM_ERROR", "Custom error with details", details);
        }

        private static class CustomObjectAccessViolation extends ObjectAccessViolation {
            public final Map<String, Object> details;
            public CustomObjectAccessViolation(String detailedHook, String customError, String customErrorWithDetails, Map<String, Object> details) {
                super(detailedHook, customError, customErrorWithDetails);
                this.details = details;
            }
        }
    }
}
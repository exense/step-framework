package step.core.objectenricher;

import org.junit.Test;
import step.core.AbstractContext;

import java.util.Map;
import java.util.Optional;
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

        Optional<ObjectAccessException> objectEditableInContext = objectHookRegistry.isObjectEditableInContext(null, null);
        assertTrue(objectEditableInContext.isEmpty());

        Optional<ObjectAccessException> objectReadableInContext = objectHookRegistry.isObjectReadableInContext(null, null);
        assertTrue(objectReadableInContext.isEmpty());

       assertTrue(objectHookRegistry.getObjectPredicate(null).test(t));
    }

    @Test
    public void testIsObjectAccessibleInContextSingleViolation() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new RestrictiveObjectHook("TenantHook", "WRONG_TENANT", "Object belongs to different tenant"));
        
        TestEnricheableObject object = new TestEnricheableObject();
        Optional<ObjectAccessException> optionalViolations = objectHookRegistry.isObjectReadableInContext(null, object);
        assertViolations(optionalViolations);
        optionalViolations = objectHookRegistry.isObjectEditableInContext(null, object);
        assertViolations(optionalViolations);
    }

    private static void assertViolations(Optional<ObjectAccessException> optionalViolations) {
        assertTrue("Should return exception with violations", optionalViolations.isPresent());
        ObjectAccessException objectAccessException = optionalViolations.get();
        assertEquals("Should have single violation", 1, objectAccessException.getViolations().size());

        ObjectAccessViolation violation = objectAccessException.getViolations().get(0);
        assertEquals("TenantHook", violation.hookId);
        assertEquals("WRONG_TENANT", violation.errorCode);
        assertEquals("Object belongs to different tenant", violation.message);

        assertEquals("Object belongs to different tenant", objectAccessException.getMessage());
    }

    @Test
    public void testIsObjectAccessibleInContextMultipleViolations() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new RestrictiveObjectHook("TenantHook", "WRONG_TENANT", "Object belongs to different tenant"));
        objectHookRegistry.add(new RestrictiveObjectHook("PermissionHook", "INSUFFICIENT_PERMISSIONS", "User lacks required permissions"));
        
        TestEnricheableObject object = new TestEnricheableObject();
        Optional<ObjectAccessException> optionalViolations = objectHookRegistry.isObjectReadableInContext(null, object);
        assertMultipleViolations(optionalViolations);

        optionalViolations = objectHookRegistry.isObjectEditableInContext(null, object);
        assertMultipleViolations(optionalViolations);
    }

    private static void assertMultipleViolations(Optional<ObjectAccessException> optionalViolations) {
        assertTrue("Should return exception with violations", optionalViolations.isPresent());
        ObjectAccessException exception = optionalViolations.get();
        assertEquals("Should have two violations", 2, exception.getViolations().size());

        assertEquals("Access denied by 2 access control rule(s)", exception.getMessage());

        List<ObjectAccessViolation> violations = exception.getViolations();
        assertEquals("TenantHook", violations.get(0).hookId);
        assertEquals("WRONG_TENANT", violations.get(0).errorCode);
        assertEquals("PermissionHook", violations.get(1).hookId);
        assertEquals("INSUFFICIENT_PERMISSIONS", violations.get(1).errorCode);
    }

    @Test
    public void testIsObjectAccessibleInContextWithDetails() {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new DetailedRestrictiveObjectHook());
        
        TestEnricheableObject object = new TestEnricheableObject();
        Optional<ObjectAccessException> optionalViolations = objectHookRegistry.isObjectReadableInContext(null, object);
        assertViolationWithDetails(optionalViolations);

        optionalViolations = objectHookRegistry.isObjectEditableInContext(null, object);
        assertViolationWithDetails(optionalViolations);
    }

    private static void assertViolationWithDetails(Optional<ObjectAccessException> optionalViolations) {
        assertTrue("Should return exception with violations", optionalViolations.isPresent());
        ObjectAccessException exception = optionalViolations.get();
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
        public Optional<ObjectAccessViolation> isObjectEditableInContext(AbstractContext context, EnricheableObject object) {
            return Optional.of(new ObjectAccessViolation(hookId, errorCode, message));
        }

        @Override
        public String getHookIdentifier() {
            return hookId;
        }

        @Override
        public Optional<ObjectAccessViolation> isObjectReadableInContext(AbstractContext context, EnricheableObject object) {
            return Optional.of(new ObjectAccessViolation(hookId, errorCode, message));
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
        public Optional<ObjectAccessViolation> isObjectEditableInContext(AbstractContext context, EnricheableObject object) {
            return getObjectAccessViolation();
        }

        private static Optional<ObjectAccessViolation> getObjectAccessViolation() {
            Map<String, Object> details = Map.of(
                    "key1", "value1",
                    "key2", "value2"
            );
            return Optional.of(new CustomObjectAccessViolation("DetailedHook", "CUSTOM_ERROR", "Custom error with details", details));
        }

        @Override
        public String getHookIdentifier() {
            return "DetailedHook";
        }

        @Override
        public Optional<ObjectAccessViolation> isObjectReadableInContext(AbstractContext context, EnricheableObject object) {
            return getObjectAccessViolation();
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
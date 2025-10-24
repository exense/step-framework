package step.core.objectenricher;

import step.core.AbstractContext;

import java.util.Objects;
import java.util.Optional;

public class WriteAccessValidatorImpl implements WriteAccessValidator {
    private final ObjectHookRegistry objectHookRegistry;
    private final AbstractContext context;

    public WriteAccessValidatorImpl(ObjectHookRegistry objectHookRegistry, AbstractContext context) {
        Objects.requireNonNull(objectHookRegistry);
        Objects.requireNonNull(context);
        this.objectHookRegistry = objectHookRegistry;
        this.context = context;
    }

    public void validate(EnricheableObject entity) throws ObjectAccessException {
        if (entity != null) {
            Optional<ObjectAccessException> optionalAccessViolation = objectHookRegistry.isObjectEditableInContext(context, entity);
            if (optionalAccessViolation.isPresent()) {
                throw optionalAccessViolation.get();
            }
        }
    }

}

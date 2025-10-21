package step.core.objectenricher;

import step.core.AbstractContext;

import java.util.Optional;

/**
 * This class is meant to be used to delegate write access validation to managers, it is constructed passing the context from the service being called
 * it validate method will return an optional ObjectAccessException
 */
public class WriteAccessValidator {
    private final ObjectHookRegistry objectHookRegistry;
    private final AbstractContext context;

    public WriteAccessValidator(ObjectHookRegistry objectHookRegistry, AbstractContext context) {
        this.objectHookRegistry = objectHookRegistry;
        this.context = context;
    }

    public Optional<ObjectAccessException> validate(EnricheableObject entity) {
        return objectHookRegistry.isObjectEditableInContext(context, entity);
    }
}

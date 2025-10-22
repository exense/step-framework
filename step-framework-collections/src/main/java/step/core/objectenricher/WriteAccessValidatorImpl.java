package step.core.objectenricher;

import step.core.AbstractContext;
import step.core.accessors.AbstractUser;

import java.util.Objects;
import java.util.Optional;

/**
 * This class is meant to be used to delegate write access validation to managers, it is constructed passing the context from the service being called
 * it validate method will return an optional ObjectAccessException
 */
public class WriteAccessValidatorImpl<U extends AbstractUser> implements WriteAccessValidator {
    private final ObjectHookRegistry<U> objectHookRegistry;
    private final AbstractContext context;
    private final U user;

    public WriteAccessValidatorImpl(ObjectHookRegistry<U> objectHookRegistry, AbstractContext context, U user) {
        Objects.requireNonNull(objectHookRegistry, "objectHookRegistry cannot be null");
        Objects.requireNonNull(context, "context cannot be null");
        Objects.requireNonNull(user, "user cannot be null");
        this.objectHookRegistry = objectHookRegistry;
        this.context = context;
        this.user = user;
    }

    @Override
    public Optional<ObjectAccessException> validateByContext(EnricheableObject entity) {
        Objects.requireNonNull(entity, "entity cannot be null");
        return objectHookRegistry.isObjectEditableInContext(context, entity);
    }

    @Override
    public Optional<ObjectAccessException> validateByUser(EnricheableObject entity) {
        Objects.requireNonNull(entity, "entity cannot be null");
        return objectHookRegistry.isObjectEditableByUser(user, entity);
    }
}

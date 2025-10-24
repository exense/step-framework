package step.core.objectenricher;

/**
 * This class is meant to be used to delegate write access validation to managers, it is constructed passing the context from the service being called
 * it validate method will return an optional ObjectAccessException
 */
public interface WriteAccessValidator {

    public static final WriteAccessValidator NO_CHECKS_VALIDATOR = new WriteAccessValidator(){};

    default public void validate(EnricheableObject entity) throws ObjectAccessException {

    }

}

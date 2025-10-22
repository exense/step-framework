package step.core.objectenricher;

import java.util.Optional;

public interface WriteAccessValidator {

    default  Optional<ObjectAccessException> validateByContext(EnricheableObject entity) {
        return Optional.empty();
    }

    default Optional<ObjectAccessException> validateByUser(EnricheableObject entity) {
        return Optional.empty();
    }
}

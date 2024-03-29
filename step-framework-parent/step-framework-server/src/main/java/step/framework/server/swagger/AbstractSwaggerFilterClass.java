package step.framework.server.swagger;

import io.swagger.v3.oas.models.Operation;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class AbstractSwaggerFilterClass {

    private static final String SPLITTER = "=";

    protected Optional<Operation> resolveOperationId(Operation operation) {
        // Create a map containing the key-value tags
        Map<String, String> attributes = operation.getTags().stream().filter(t -> t.contains(SPLITTER)).collect(Collectors.toMap(t -> t.split(SPLITTER)[0], t -> t.split(SPLITTER)[1]));
        // Replace the placeholders in the operation id
        String operationId = operation.getOperationId();
        String newOperationId = operationId;
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            newOperationId = newOperationId.replaceAll("\\{" + attribute.getKey() + "\\}", attribute.getValue());
        }

        // If the operation id has been changed (i.e. a placeholder has been replaced), we remove the suffix _1 added by Swagger
        if (!operationId.equals(newOperationId)) {
            newOperationId = newOperationId.replaceAll("_[0-9]+$", "");
        }

        // Remove key-value tags from the list of tags
        List<String> newTags = operation.getTags().stream().filter(t -> !t.contains(SPLITTER)).collect(Collectors.toList());
        operation.setTags(newTags);
        return Optional.of(operation.operationId(newOperationId));
    }
}

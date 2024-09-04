package step.framework.server.swagger;

import io.swagger.v3.core.filter.OpenAPISpecFilter;
import io.swagger.v3.core.model.ApiDescription;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SwaggerFilterClass extends AbstractSwaggerFilterClass implements OpenAPISpecFilter {
	@Override
	public Optional<OpenAPI> filterOpenAPI(OpenAPI openAPI, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(openAPI);
	}

	@Override
	public Optional<PathItem> filterPathItem(PathItem pathItem, ApiDescription apiDescription, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(pathItem);
	}

	@Override
	public Optional<Operation> filterOperation(Operation operation, ApiDescription apiDescription, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		if (operation.getTags() != null && operation.getTags().stream().filter(t -> t.startsWith("Private ")).findFirst().isEmpty()) {
			return resolveOperationId(operation);
		} else {
			return Optional.empty();
		}
	}

	@Override
	public Optional<Parameter> filterParameter(Parameter parameter, Operation operation, ApiDescription apiDescription, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(parameter);
	}

	@Override
	public Optional<RequestBody> filterRequestBody(RequestBody requestBody, Operation operation, ApiDescription apiDescription, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(requestBody);
	}

	@Override
	public Optional<ApiResponse> filterResponse(ApiResponse apiResponse, Operation operation, ApiDescription apiDescription, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(apiResponse);
	}

	@Override
	public Optional<Schema> filterSchema(Schema schema, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(schema);
	}

	@Override
	public Optional<Schema> filterSchemaProperty(Schema schema, Schema schema1, String s, Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>> map2) {
		return Optional.of(schema);
	}

	@Override
	public boolean isRemovingUnreferencedDefinitions() {
		return false;
	}
}

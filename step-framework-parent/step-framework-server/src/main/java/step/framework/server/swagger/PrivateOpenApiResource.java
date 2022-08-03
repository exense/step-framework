package step.framework.server.swagger;

import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import jakarta.ws.rs.Path;

@Path("/private-openapi.{type:json|yaml}")
public class PrivateOpenApiResource extends OpenApiResource {

}

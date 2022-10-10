package step.framework.server.tables;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import step.framework.server.AbstractServices;
import step.framework.server.security.Secured;
import step.framework.server.security.SecuredContext;

@Tag(name = "Functions")
@Tag(name = "Entity=Function")
@Singleton
@Path("functions")
@SecuredContext(key = "prefix", value = "functions")
public class TestService extends AbstractServices {

    @GET
    @Path("/serviceWithMultipleRights")
    @Secured(right = "{prefix}-read")
    @Secured(right = "test")
    @Operation(operationId = "get{Entity}")
    public String serviceWithMultipleRights() {
        return "test";
    }

    @GET
    @Path("/serviceWithMultipleRightsNotAllowed")
    @Secured(right = "{prefix}-read")
    @Secured(right = "notAvailableRight")
    @Operation(operationId = "get{Entity}")
    public String serviceWithMultipleRightsNotAllowed() {
        return "test";
    }

    @GET
    @Path("/serviceWithOneRight")
    @Secured(right = "test")
    @Operation(operationId = "get{Entity}")
    public String serviceWithOneRight() {
        return "test";
    }

    @GET
    @Path("/serviceWithOneRightNotAllowed")
    @Secured(right = "notAvailableRight")
    @Operation(operationId = "get{Entity}")
    public String serviceWithOneRightNotAllowed() {
        return "test";
    }

    @GET
    @Path("/login")
    public String login() {
        getSession().setAuthenticated(true);
        return "test";
    }

}

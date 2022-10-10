package step.framework.server.tables;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import step.framework.server.AbstractServices;

@Tag(name = "Functions")
@Tag(name = "Entity=Plans")
@Singleton
@Path("plans")
public class TestService2 extends AbstractServices {

    @GET
    @Path("/")
    @Operation(operationId = "get{Entity}")
    public String get() {
        return "test";
    }

}

package step.framework.server.security;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Asserts that the providers name bound to {@link Secured} are applied to all the secured services, whether they
 * declare one or several {@link Secured} annotations.
 */
public class SecuredNameBindingTest {

    private static final URI BASE_URI = URI.create("http://localhost/");

    private ApplicationHandler applicationHandler;

    @Before
    public void before() {
        SecuredFilter.securedServices.clear();
        applicationHandler = new ApplicationHandler(new ResourceConfig(TestService.class, SecuredFilter.class,
            SecuredDynamicFeature.class));
    }

    @Test
    public void filterIsNotAppliedToUnsecuredServices() throws Exception {
        assertEquals(200, call("services/unsecured"));
        assertEquals(List.of(), SecuredFilter.securedServices);
    }

    @Test
    public void filterIsAppliedToServicesWithOneRight() throws Exception {
        assertEquals(200, call("services/oneRight"));
        assertEquals(List.of("services/oneRight"), SecuredFilter.securedServices);
    }

    @Test
    public void filterIsAppliedToServicesWithMultipleRights() throws Exception {
        assertEquals(200, call("services/multipleRights"));
        assertEquals(List.of("services/multipleRights"), SecuredFilter.securedServices);
    }

    @Test
    public void filterIsAppliedOnceToServicesWithMultipleRights() throws Exception {
        call("services/multipleRights");
        call("services/multipleRights");
        assertEquals(List.of("services/multipleRights", "services/multipleRights"), SecuredFilter.securedServices);
    }

    private int call(String path) throws Exception {
        ContainerRequest request = new ContainerRequest(BASE_URI, BASE_URI.resolve(path), "GET", null,
            new MapPropertiesDelegate(), null);
        ContainerResponse response = applicationHandler.apply(request).get();
        return response.getStatus();
    }

    @Path("/services")
    public static class TestService {

        @GET
        @Path("/unsecured")
        public String unsecured() {
            return "test";
        }

        @GET
        @Path("/oneRight")
        @Secured(right = "right1")
        public String oneRight() {
            return "test";
        }

        @GET
        @Path("/multipleRights")
        @Secured(right = "right1")
        @Secured(right = "right2")
        public String multipleRights() {
            return "test";
        }
    }

    @Secured
    @Provider
    public static class SecuredFilter implements ContainerRequestFilter {

        private static final List<String> securedServices = new ArrayList<>();

        @Override
        public void filter(ContainerRequestContext requestContext) {
            securedServices.add(requestContext.getUriInfo().getPath());
        }
    }
}

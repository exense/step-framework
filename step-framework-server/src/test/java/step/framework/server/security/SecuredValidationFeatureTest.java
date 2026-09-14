package step.framework.server.security;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Asserts that the services overriding a secured service without declaring the rights they require are detected,
 * whether they are left unsecured or secured by rights not visible in their code.
 */
public class SecuredValidationFeatureTest {

    @Test
    public void servicesOverridingNoServiceAreNotReported() throws Exception {
        assertEquals(List.of(), overriddenRights(EntityServices.class, "get", String.class));
    }

    @Test
    public void unsecuredServicesAreNotReported() throws Exception {
        assertEquals(List.of(), overriddenRights(BareOverridingServices.class, "unsecured"));
    }

    @Test
    public void servicesOverridingASecuredServiceAreReported() throws Exception {
        assertEquals(List.of("entity-read"), overriddenRights(BareOverridingServices.class, "get", String.class));
    }

    @Test
    public void servicesOverridingAServiceRequiringSeveralRightsAreReported() throws Exception {
        assertEquals(List.of("entity-read", "entity-write"),
            overriddenRights(BareOverridingServices.class, "restore", String.class));
    }

    @Test
    public void servicesRedeclaringTheJaxRsAnnotationsAreReported() throws Exception {
        // The dangerous case: declaring a JAX-RS annotation drops the inherited annotations, the service isn't even
        // bound to the security providers anymore
        assertEquals(List.of("entity-read"), overriddenRights(JaxRsRedeclaringServices.class, "get", String.class));
        assertEquals(List.of("entity-read"), overriddenRights(ProducesRedeclaringServices.class, "get", String.class));
    }

    @Test
    public void servicesOverridingAGenericServiceAreReported() throws Exception {
        // The overriding method has a narrower parameter type than the generic method it overrides
        assertEquals(List.of("generic-write"), overriddenRights(GenericOverridingServices.class, "save", String.class));
    }

    @Test
    public void servicesOverridingADeeplyOverriddenServiceAreReported() throws Exception {
        // A extends B extends C, only C declaring the rights
        assertEquals(List.of("deep-read"), overriddenRights(ServicesA.class, "get", String.class));
    }

    @Test
    public void servicesImplementingASecuredInterfaceAreReported() throws Exception {
        assertEquals(List.of("interface-read"), overriddenRights(InterfaceImplementingServices.class, "read"));
    }

    @Test
    public void undeclaredRightsMakeTheApplicationStartupFail() {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> new ApplicationHandler(new ResourceConfig(JaxRsRedeclaringServices.class, SecuredValidationFeature.class)));
        assertTrue(exception.getMessage().contains("JaxRsRedeclaringServices.get overrides a secured service without " +
            "declaring the rights it requires"));
        assertTrue(exception.getMessage().contains("@Secured(right = \"entity-read\")"));
    }

    @Test
    public void divergingRightsAreReportedWhenTheApplicationStarts() {
        List<String> messages = messages(Level.DEBUG, DivergingServices.class);
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).contains("DivergingServices.get"));
        assertTrue(messages.get(0).contains(
            "requires the rights [custom-read] while the service it overrides requires [entity-read]"));
    }

    @Test
    public void servicesDeclaringTheirRightsDoNotMakeTheApplicationStartupFail() {
        // The application starts, without reporting anything
        assertEquals(List.of(), messages(Level.DEBUG, RedeclaringServices.class));
    }

    private List<String> messages(Level level, Class<?> serviceClass) {
        Logger logger = (Logger) LoggerFactory.getLogger(SecuredValidationFeature.class);
        Level initialLevel = logger.getLevel();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        try {
            new ApplicationHandler(new ResourceConfig(serviceClass, SecuredValidationFeature.class));
        } finally {
            logger.setLevel(initialLevel);
            logger.detachAppender(appender);
        }
        return appender.list.stream()
            .filter(event -> event.getLevel() == level)
            .map(ILoggingEvent::getFormattedMessage)
            .collect(Collectors.toList());
    }

    private List<String> overriddenRights(Class<?> serviceClass, String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = serviceClass.getDeclaredMethod(methodName, parameterTypes);
        return SecuredValidationFeature.findOverriddenRights(method);
    }

    @Path("/entities")
    public static class EntityServices {

        @GET
        @Path("/unsecured")
        public String unsecured() {
            return "test";
        }

        @GET
        @Path("/{id}")
        @Secured(right = "entity-read")
        public String get(String id) {
            return id;
        }

        @GET
        @Path("/{id}/restore")
        @Secured(right = "entity-read")
        @Secured(right = "entity-write")
        public String restore(String id) {
            return id;
        }
    }

    @Path("/bare")
    public static class BareOverridingServices extends EntityServices {

        @Override
        public String unsecured() {
            return "overridden";
        }

        @Override
        @Operation(description = "a non JAX-RS annotation doesn't drop the inherited ones")
        public String get(String id) {
            return "overridden";
        }

        @Override
        public String restore(String id) {
            return "overridden";
        }
    }

    @Path("/redeclaring")
    public static class RedeclaringServices extends EntityServices {

        @Override
        @Secured(right = "entity-read")
        public String get(String id) {
            return "overridden";
        }
    }

    @Path("/diverging")
    public static class DivergingServices extends EntityServices {

        @Override
        @Secured(right = "custom-read")
        public String get(String id) {
            return "overridden";
        }
    }

    @Path("/jaxRsRedeclaring")
    public static class JaxRsRedeclaringServices extends EntityServices {

        @Override
        @GET
        public String get(String id) {
            return "overridden";
        }
    }

    @Path("/producesRedeclaring")
    public static class ProducesRedeclaringServices extends EntityServices {

        @Override
        @Produces(MediaType.APPLICATION_JSON)
        public String get(String id) {
            return "overridden";
        }
    }

    public static abstract class GenericServices<T> {

        @GET
        @Path("/save")
        @Secured(right = "generic-write")
        public T save(T entity) {
            return entity;
        }
    }

    @Path("/generic")
    public static class GenericOverridingServices extends GenericServices<String> {

        @Override
        public String save(String entity) {
            return "overridden";
        }
    }

    public static abstract class ServicesC {

        @GET
        @Path("/{id}")
        @Secured(right = "deep-read")
        public String get(String id) {
            return id;
        }
    }

    public static class ServicesB extends ServicesC {

        @Override
        public String get(String id) {
            return "b";
        }
    }

    @Path("/a")
    public static class ServicesA extends ServicesB {

        @Override
        public String get(String id) {
            return "a";
        }
    }

    public interface ReadableServices {

        @GET
        @Path("/read")
        @Secured(right = "interface-read")
        String read();
    }

    @Path("/interface")
    public static class InterfaceImplementingServices implements ReadableServices {

        @Override
        public String read() {
            return "test";
        }
    }
}

package step.framework.server;

import ch.exense.commons.app.Configuration;
import step.core.AbstractContext;
import step.core.Version;
import step.core.access.Role;
import step.core.access.RoleResolver;
import step.core.plugins.Plugin;
import step.framework.server.access.AccessManager;
import step.framework.server.security.SecurityFilter;
import step.framework.server.tables.TestService;
import step.framework.server.tables.TestService2;

@Plugin
public class ControllerServerTest implements ServerPlugin<AbstractContext> {

    public static void main(String[] args) throws Exception {
        new ControllerServer(new Configuration()).start();
    }

    @Override
    public void serverStart(AbstractContext context) throws Exception {
        context.put(Version.class, new Version(0,0,0));
        context.put(AccessManager.class, new AccessManager() {
            @Override
            public void setRoleResolver(RoleResolver roleResolver) {

            }

            @Override
            public boolean checkRightInContext(Session session, String right) {
                return right.equals("test") || right.equals("functions-read");
            }

            @Override
            public Role getRoleInContext(Session session) {
                return null;
            }
        });
        context.require(ServiceRegistrationCallback.class).registerService(SecurityFilter.class);
        context.require(ServiceRegistrationCallback.class).registerService(TestService.class);
        context.require(ServiceRegistrationCallback.class).registerService(TestService2.class);
    }

    @Override
    public void migrateData(AbstractContext context) throws Exception {

    }

    @Override
    public void initializeData(AbstractContext context) throws Exception {

    }

    @Override
    public void afterInitializeData(AbstractContext context) throws Exception {

    }

    @Override
    public void serverStop(AbstractContext context) {

    }

    @Override
    public boolean canBeDisabled() {
        return false;
    }
}

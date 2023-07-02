package step.framework.server.access;

import step.core.access.Role;
import step.core.access.RoleResolver;
import step.framework.server.Session;

public class NoAuthorizationManager implements AuthorizationManager {
	@Override
	public void setRoleResolver(RoleResolver roleResolver) {

	}

	@Override
	public boolean checkRightInContext(Session session, String right) {
		return true;
	}

	@Override
	public boolean checkRightInContext(Session session, String right, String userIdOnBehalfOf) {
		return true;
	}

	@Override
	public boolean checkRightInRole(String role, String right) {
		return true;
	}

	@Override
	public Role getRoleInContext(Session session) {
		Role role = new Role();
		role.addAttribute("name","admin");
		return role;
	}

}

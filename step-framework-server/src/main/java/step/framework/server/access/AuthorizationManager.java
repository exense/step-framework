/*******************************************************************************
 * Copyright (C) 2020, exense GmbH
 *  
 * This file is part of STEP
 *  
 * STEP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *  
 * STEP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *  
 * You should have received a copy of the GNU Affero General Public License
 * along with STEP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package step.framework.server.access;

import step.core.access.Role;
import step.core.access.RoleResolver;
import step.core.accessors.AbstractUser;
import step.core.objectenricher.EnricheableObject;
import step.framework.server.Session;

public interface AuthorizationManager<U extends AbstractUser, S extends Session<U>>  {

	void setRoleResolver(RoleResolver roleResolver);

	boolean checkRightInContext(S session, String right);

	default boolean checkRightInContext(S session, String right, String usernameOnBehalfOf) {
		// onBehalfOf is not supported by default
		if (usernameOnBehalfOf == null || usernameOnBehalfOf.isEmpty()) {
			return checkRightInContext(session, right);
		} else {
			throw new UnsupportedOperationException("Authorization 'on behalf of' is not supported in " + this.getClass().getSimpleName());
		}
	}

	boolean checkRightInRole(String role, String right);

    boolean checkRightInRoleForEntity(String role, String rightSuffix, EnricheableObject object);

	Role getRoleInContext(S session);

}

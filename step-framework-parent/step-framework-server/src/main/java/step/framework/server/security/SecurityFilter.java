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
package step.framework.server.security;

import java.io.IOException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.model.Invocable;

import step.core.accessors.AbstractUser;
import step.framework.server.AbstractServices;
import step.framework.server.Session;
import step.framework.server.access.AccessManager;

@Provider
@Priority(Priorities.AUTHORIZATION)
public class SecurityFilter<U extends AbstractUser> extends AbstractServices<U> implements ContainerRequestFilter {
	
	@Inject
	private ExtendedUriInfo extendendUriInfo;

	private AccessManager accessManager;

	@PostConstruct
	public void init() throws Exception {
		accessManager = getAbstractContext().require(AccessManager.class);
	}
	
	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		// Retrieve or initialize session
		Session session = retrieveOrInitializeSession();

		// Check rights
		Invocable invocable = extendendUriInfo.getMatchedResourceMethod().getInvocable();
		Secured classAnnotation = invocable.getHandler().getHandlerClass().getAnnotation(Secured.class);
		Secured annotation = invocable.getHandlingMethod().getAnnotation(Secured.class);
		if(annotation != null) {
			if(session.isAuthenticated()) {
				String right = annotation.right();
				if(right.length()>0) {
					if (classAnnotation != null && classAnnotation.right().length()>0) {
						right = classAnnotation.right() + right;
					}
					boolean hasRight = accessManager.checkRightInContext(session, right);
					if(!hasRight) {
						requestContext.abortWith(Response.status(Response.Status.FORBIDDEN).build());
					}
				}
			} else {
				requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
			}
		}
	}
	
	protected Session retrieveOrInitializeSession() {
		Session session = getSession();
		if(session == null) {
			session = new Session();
			setSession(session);
		}
		return session;
	}
}

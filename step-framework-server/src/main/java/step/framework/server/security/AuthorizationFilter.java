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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.accessors.AbstractUser;
import step.framework.server.AbstractServices;
import step.framework.server.Session;
import step.framework.server.access.AuthorizationManager;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Authentication filters for secured services
 * User must be authenticated, if the endpoint requires specific rights,
 * the access rights validation is delegated to the AuthorizationManager
 */
@Secured
@Provider
@Priority(Priorities.AUTHORIZATION)
public class AuthorizationFilter<U extends AbstractUser> extends AbstractServices<U> implements ContainerRequestFilter {

    @Inject
    private ExtendedUriInfo extendendUriInfo;

    private AuthorizationManager authorizationManager;

    private static final Logger logger = LoggerFactory.getLogger(AuthorizationFilter.class);

    @PostConstruct
    public void init() throws Exception {
        authorizationManager = getAbstractContext().require(AuthorizationManager.class);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Session<U> session = getSession();
        Invocable invocable = extendendUriInfo.getMatchedResourceMethod().getInvocable();
        Class<?> handlerClass = invocable.getHandler().getHandlerClass();

        if (!session.isAuthenticated()) {
            // Session not authenticated
            if (logger.isDebugEnabled()) {
                logger.debug("User '" + username(session) + "' not authenticated. Returning " + Response.Status.UNAUTHORIZED);
            }
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
            return;
        }

        // Session authenticated, check each right required by the service
        getRequiredRights(invocable, handlerClass)
            .forEach(right -> checkRight(requestContext, session, handlerClass, right));
    }

    /**
     * Collects the rights declared by the matched service. The annotation is looked up on the handling method first
     * and on the handler class otherwise, matching the elements the filter can be name bound to.
     */
    private List<String> getRequiredRights(Invocable invocable, Class<?> handlerClass) {
        Secured secured = invocable.getHandlingMethod().getAnnotation(Secured.class);
        if (secured == null) {
            secured = handlerClass.getAnnotation(Secured.class);
        }
        if (secured == null) {
            return List.of();
        }
        return Stream.concat(Stream.of(secured.right()), Arrays.stream(secured.rights()))
            .filter(right -> !right.isEmpty())
            .collect(Collectors.toList());
    }

    private void checkRight(ContainerRequestContext requestContext, Session<?> session, Class<?> handlerClass, String right) {
        // Replacing placeholders in right based on SecuredContext annotations
        // Setting allowAllSignedInUsers flag based on secured context
        boolean allowAllSignedInUsers = false;
        Annotation[] handlerClassAnnotations = handlerClass.getAnnotations();
        for (Annotation a : handlerClassAnnotations) {
            if (a instanceof SecuredContext) {
                SecuredContext securedContext = (SecuredContext) a;
                allowAllSignedInUsers = securedContext.allowAllSignedInUsers();
                right = right.replaceAll(Pattern.quote("{" + securedContext.key() + "}"), securedContext.value());
            }
        }
        // Check resolved right
        boolean hasRight = allowAllSignedInUsers || authorizationManager.checkRightInContext(session, right);
        if (logger.isDebugEnabled()) {
            logger.debug("Checked right '" + right + "' for user '" + username(session) + "'. Result: " + hasRight);
        }
        if (!hasRight) {
            requestContext.abortWith(Response.status(Response.Status.FORBIDDEN).build());
            logger.warn("User " + username(session) + " missing right " + right);
        }
    }

    private String username(Session<?> session) {
        return (session != null && session.getUser() != null) ?
            session.getUser().getSessionUsername() :
            null;
    }

}

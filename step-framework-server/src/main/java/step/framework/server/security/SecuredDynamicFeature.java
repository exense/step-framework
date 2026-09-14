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

import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.ext.Provider;
import jakarta.ws.rs.ext.ReaderInterceptor;
import jakarta.ws.rs.ext.WriterInterceptor;

/**
 * Binds the providers name bound to {@link Secured} to the services declaring several rights.
 * <p>
 * {@link Secured} is repeatable, and repeated annotations are replaced by their container annotation at compile time:
 * a service declaring several rights is annotated with {@link SecuredList} and not with {@link Secured}. Since JAX-RS
 * resolves name binding on the annotations actually present on the method, such services would otherwise be bound to
 * no provider at all, silently bypassing authentication and authorization.
 */
@Provider
public class SecuredDynamicFeature implements DynamicFeature {

    /**
     * The configuration of the application, i.e. all the registered providers. The configuration exposed by the
     * FeatureContext is the one of the service being configured and doesn't contain them.
     */
    @Context
    private Configuration configuration;

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        if (resourceInfo.getResourceMethod() == null || !resourceInfo.getResourceMethod().isAnnotationPresent(SecuredList.class)) {
            return;
        }
        // Bind the providers name bound to Secured to this service, as JAX-RS does for the services
        // declaring a single right
        configuration.getClasses().stream()
            .filter(SecuredDynamicFeature::isSecuredProvider)
            .forEach(context::register);
        configuration.getInstances().stream()
            .filter(instance -> isSecuredProvider(instance.getClass()))
            .forEach(context::register);
    }

    private static boolean isSecuredProvider(Class<?> providerClass) {
        return providerClass.isAnnotationPresent(Secured.class) && (
            ContainerRequestFilter.class.isAssignableFrom(providerClass) ||
                ContainerResponseFilter.class.isAssignableFrom(providerClass) ||
                ReaderInterceptor.class.isAssignableFrom(providerClass) ||
                WriterInterceptor.class.isAssignableFrom(providerClass));
    }
}

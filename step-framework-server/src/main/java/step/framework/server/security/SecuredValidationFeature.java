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

import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The sole purpose of this class is to detect potential services lacking or misconfigured {@link Secured} annotation.
 * It reports services overriding base services with different @Secured annotation, this is checked when the application starts.
 * <p>
 * Only the @Secured annotation of the invoked method is resolved, the one on the base method is ignored.
 * Not redeclaring the @Secured annotation means the service is not secured at all (unless the annotation is on the class),
 * in any case no rights are asserted, this makes the application startup fail.
 * Likewise, defining a right different from the one on the base method might be unexpected and is reported as debug.
 */
@Provider
public class SecuredValidationFeature implements DynamicFeature {

    private static final Logger logger = LoggerFactory.getLogger(SecuredValidationFeature.class);

    private final Set<Method> reportedMethods = ConcurrentHashMap.newKeySet();

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context) {
        Method resourceMethod = resourceInfo.getResourceMethod();
        if (resourceMethod == null || !reportedMethods.add(resourceMethod)) {
            return;
        }
        List<String> overriddenRights = findOverriddenRights(resourceMethod);
        if (overriddenRights.isEmpty()) {
            // The service overrides no secured service, it declares its rights or requires none
            return;
        }
        List<String> declaredRights = rightsOf(resourceMethod.getAnnotationsByType(Secured.class));
        if (declaredRights.isEmpty()) {
            failOnUndeclaredRights(resourceMethod, overriddenRights);
        } else if (!declaredRights.containsAll(overriddenRights)) {
            reportDivergingRights(resourceMethod, declaredRights, overriddenRights);
        }
    }

    /**
     * Fails the application startup when the annotation or the right definition is missing
     */
    private void failOnUndeclaredRights(Method resourceMethod, List<String> overriddenRights) {
        throw new IllegalStateException("The service " + serviceName(resourceMethod) + " overrides a secured service " +
            "without declaring the rights it requires. Declare the following annotation(s) on this service: " +
            annotations(overriddenRights));
    }

    /**
     * Report when the overriding method declare other rights than the one of the method it overrides
     */
    private void reportDivergingRights(Method resourceMethod, List<String> declaredRights, List<String> overriddenRights) {
        logger.debug("The service " + serviceName(resourceMethod) + " requires the rights " + declaredRights +
            " while the service it overrides requires " + overriddenRights + ". Only the rights declared by this " +
            "service are checked, verify they are the intended ones.");
    }

    /**
     * Returns the rights declared by the closest service overridden by the given service, empty if it overrides no
     * secured service. The rights are resolved in the class hierarchy rather than by JAX-RS, which resolves nothing
     * at all for the services declaring their own JAX-RS annotations.
     */
    static List<String> findOverriddenRights(Method resourceMethod) {
        return findOverriddenRights(resourceMethod.getDeclaringClass(), resourceMethod);
    }

    private static List<String> findOverriddenRights(Class<?> clazz, Method resourceMethod) {
        List<Class<?>> superTypes = new ArrayList<>();
        if (clazz.getSuperclass() != null) {
            superTypes.add(clazz.getSuperclass());
        }
        superTypes.addAll(List.of(clazz.getInterfaces()));
        for (Class<?> superType : superTypes) {
            Method overriddenMethod = findOverriddenMethod(superType, resourceMethod);
            if (overriddenMethod != null) {
                List<String> rights = rightsOf(overriddenMethod.getAnnotationsByType(Secured.class));
                if (!rights.isEmpty()) {
                    return rights;
                }
            }
            List<String> rights = findOverriddenRights(superType, resourceMethod);
            if (!rights.isEmpty()) {
                return rights;
            }
        }
        return List.of();
    }

    /**
     * Returns the method declared by the given class the given method overrides, if any. The parameters of a method
     * overriding a generic service are narrower than the ones of the declaration it overrides, hence the fallback on
     * the compatible signature.
     */
    private static Method findOverriddenMethod(Class<?> clazz, Method resourceMethod) {
        Method compatibleMethod = null;
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
            if (!declaredMethod.getName().equals(resourceMethod.getName())
                || declaredMethod.getParameterCount() != resourceMethod.getParameterCount()) {
                continue;
            }
            if (Arrays.equals(declaredMethod.getParameterTypes(), resourceMethod.getParameterTypes())) {
                return declaredMethod;
            }
            if (compatibleMethod == null && hasCompatibleParameters(declaredMethod, resourceMethod)) {
                compatibleMethod = declaredMethod;
            }
        }
        return compatibleMethod;
    }

    private static boolean hasCompatibleParameters(Method declaredMethod, Method resourceMethod) {
        Class<?>[] declaredParameterTypes = declaredMethod.getParameterTypes();
        Class<?>[] parameterTypes = resourceMethod.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (!declaredParameterTypes[i].isAssignableFrom(parameterTypes[i])) {
                return false;
            }
        }
        return true;
    }

    private static List<String> rightsOf(Secured[] securedAnnotations) {
        return Arrays.stream(securedAnnotations)
            .map(Secured::right)
            .filter(right -> !right.isEmpty())
            .distinct()
            .collect(Collectors.toList());
    }

    private static String annotations(List<String> rights) {
        return rights.stream()
            .map(right -> "@Secured(right = \"" + right + "\")")
            .collect(Collectors.joining(" "));
    }

    private static String serviceName(Method resourceMethod) {
        return resourceMethod.getDeclaringClass().getName() + "." + resourceMethod.getName();
    }
}

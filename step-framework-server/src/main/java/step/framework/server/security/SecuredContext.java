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

import jakarta.ws.rs.NameBinding;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the value of a placeholder used in the rights required by the services, allowing generic services to
 * declare the rights of the entity they are used for. The right {@code {entity}-read} declared by a service of a
 * class annotated with {@code @SecuredContext(key = "entity", value = "plan")} resolves to {@code plan-read}.
 * <p>
 * The context is declared on a service class and applies to all its services. A service requiring the rights of
 * another entity than the one of its class declares that right explicitly rather than using a placeholder.
 *
 * @see Secured
 */
@NameBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface SecuredContext {

    /**
     * The name of the placeholder, i.e. {@code entity} for the placeholder {@code {entity}}.
     */
    String key();

    /**
     * The value replacing the placeholder in the rights required by the services.
     */
    String value();

    /**
     * Whether all the signed in users are allowed to call the services, i.e. their rights are not checked.
     */
    boolean allowAllSignedInUsers() default false;
}

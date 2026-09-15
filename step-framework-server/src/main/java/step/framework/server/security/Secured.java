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

import java.lang.annotation.*;

/**
 * Marks a service as secured: the security providers are name bound to this annotation and only apply to the
 * annotated services, which require an authenticated session and the declared right, if any.
 * <p>
 * A whole class can be annotated to secure all the services it declares at once. The rights are however only
 * resolved on the services themselves, thus a right declared on the class is not checked: every service requiring a
 * specific right declares it.
 * <p>
 * The annotation is repeatable, a service declaring several of them requires all the declared rights. Note that
 * repeated annotations are replaced by their container annotation {@link SecuredList} at compile time, i.e. such a
 * service is not annotated with Secured at all: the SecuredDynamicFeature binds the security providers to them.
 * <p>
 * Every service declares the rights it requires, the services overriding another service included.
 *
 * @see SecuredContext for the placeholders which can be used in the rights
 */
@NameBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Repeatable(value = SecuredList.class)
public @interface Secured {

    /**
     * The right required to call the service, none if empty, i.e. an authenticated session is sufficient. The right
     * may contain placeholders resolved against the {@link SecuredContext} of the service class.
     */
    String right() default "";

}

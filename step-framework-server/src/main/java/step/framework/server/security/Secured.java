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
 * Marks a service as secured: the security filters are bound to this annotation and only apply to annotated
 * services. Use {@link #rights()} to require several rights at once.
 * <p>
 * This annotation is deliberately not repeatable: a repeated annotation is replaced by its container annotation
 * at compile time, which would drop the JAX-RS name binding and silently disable all the security filters.
 */
@NameBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Secured {

    String right() default "";

    String[] rights() default {};

}

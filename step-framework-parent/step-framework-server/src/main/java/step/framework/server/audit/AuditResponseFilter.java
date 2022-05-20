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
package step.framework.server.audit;

import step.core.accessors.AbstractUser;
import step.framework.server.AbstractServices;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
public class AuditResponseFilter<U extends AbstractUser> extends AbstractServices<U> implements ContainerResponseFilter {

	@Context
	private HttpServletRequest sr;
	
	@PostConstruct
	public void init() throws Exception {

	}

	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {
		AuditLogger.logResponse(sr, responseContext.getStatus());
	}
}

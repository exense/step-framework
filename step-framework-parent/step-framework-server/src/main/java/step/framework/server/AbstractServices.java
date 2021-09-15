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
package step.framework.server;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public abstract class AbstractServices {

	private static final String SESSION = "session";

	@Inject
	private ServerContext serverContext;

	@Inject
	private HttpServletRequest httpServletRequest;

	public ServerContext getServerContext() {
		return serverContext;
	}
	
	//required for unit test
	protected void setHttpServletRequest(HttpServletRequest httpServletRequest) {
		this.httpServletRequest = httpServletRequest;
	}

	public HttpSession getHttpSession() {
		return httpServletRequest.getSession();
	}

	protected Session getSession() {
		HttpSession httpSession = getHttpSession();
		if (httpSession != null) {
			return (Session) httpSession.getAttribute(SESSION);
		} else {
			return null;
		}
	}

	protected void setSession(Session session) {
		getHttpSession().setAttribute(SESSION, session);
	}
}
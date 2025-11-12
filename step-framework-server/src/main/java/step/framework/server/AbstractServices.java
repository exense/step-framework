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

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import step.core.AbstractContext;
import step.core.accessors.AbstractUser;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public abstract class AbstractServices<U extends AbstractUser> {

	private static final String SESSION = "session";

	@Inject
	private AbstractContext context;

	@Inject
	private HttpServletRequest httpServletRequest;

	public AbstractContext getAbstractContext() {
		return context;
	}

	//required for unit test
	protected void setHttpServletRequest(HttpServletRequest httpServletRequest) {
		this.httpServletRequest = httpServletRequest;
	}

	/**
	 * Get the HTTP session, a session is created automatically if none exists
	 * @return the HTTP session
	 */
	public HttpSession getHttpSession() {
		return httpServletRequest.getSession();
	}

	/**
	 * Get the HTTP session if it exists
	 * @return the HTTP session if it exists or null
	 */
	private HttpSession getHttpSessionIfExists() {
		return httpServletRequest.getSession(false);
	}

	/**
	 * Get the Step session if it exists without implicitly creating an HTTP session when none exists
	 * @return the session or null if it doesn't exist
	 */
	protected Session<U> getSessionIfExists() {
		HttpSession httpSession = getHttpSessionIfExists();
		if (httpSession != null) {
			return (Session) httpSession.getAttribute(SESSION);
		} else {
			return null;
		}
	}

	protected Session<U> getSession() {
		HttpSession httpSession = getHttpSession();
		if (httpSession != null) {
			return (Session) httpSession.getAttribute(SESSION);
		} else {
			return null;
		}
	}

	protected void setSession(Session<U> session) {
		getHttpSession().setAttribute(SESSION, session);
	}

	protected void invalidateSession(){
		getHttpSession().invalidate();
	}

	public static Session getSession(HttpSession httpSession) {
		if (httpSession != null) {
			return (Session) httpSession.getAttribute(SESSION);
		} else {
			return null;
		}
	}
}

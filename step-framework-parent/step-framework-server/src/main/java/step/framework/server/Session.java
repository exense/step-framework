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

import step.core.AbstractContext;
import step.core.accessors.AbstractUser;
import step.framework.server.access.TokenType;

public class Session<U extends AbstractUser> extends AbstractContext {

	private U user;

	private boolean authenticated;

	protected String token;

	protected TokenType tokenType;

	public Session() {
		super();
	}

	public U getUser() {
		return user;
	}

	public void setUser(U user) {
		this.user = user;
	}

	public boolean isAuthenticated() {
		return authenticated;
	}

	public void setAuthenticated(boolean authenticated) {
		this.authenticated = authenticated;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public TokenType getTokenType() {
		return tokenType;
	}

	public void setTokenType(TokenType tokenType) {
		this.tokenType = tokenType;
	}
}

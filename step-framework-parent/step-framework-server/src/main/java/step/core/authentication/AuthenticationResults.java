/*******************************************************************************
 * Copyright 2021 exense GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package step.core.authentication;

import step.framework.server.access.TokenType;

import java.util.Map;

public class AuthenticationResults {

    boolean authenticated;

    String username;

    String mainRole;

    AuthenticationTokenDetails authenticationTokenDetails;

    String token;

    TokenType tokenType;

    Map<String, Object> sessionStore;

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getMainRole() {
        return mainRole;
    }

    public void setMainRole(String mainRole) {
        this.mainRole = mainRole;
    }

    public AuthenticationTokenDetails getAuthenticationTokenDetails() {
        return authenticationTokenDetails;
    }

    public void setAuthenticationTokenDetails(AuthenticationTokenDetails authenticationTokenDetails) {
        this.authenticationTokenDetails = authenticationTokenDetails;
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

    public Map<String, Object> getSessionStore() {
        return sessionStore;
    }

    public void setSessionStore(Map<String, Object> sessionStore) {
        this.sessionStore = sessionStore;
    }
}

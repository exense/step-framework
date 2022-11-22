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

import java.util.Map;

public class AuthenticationResults {

    boolean authenticated;

    AuthenticationTokenDetails authenticationTokenDetails;

    String token;

    boolean isLocalToken;

    Map<String, Object> sessionStore;

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
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

    public boolean isLocalToken() {
        return isLocalToken;
    }

    public void setLocalToken(boolean localToken) {
        isLocalToken = localToken;
    }

    public Map<String, Object> getSessionStore() {
        return sessionStore;
    }

    public void setSessionStore(Map<String, Object> sessionStore) {
        this.sessionStore = sessionStore;
    }
}

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

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AuthenticationTokenDetails {
    private final String id;
    private final String username;
    private final String role;
    private final Map<String, List<String>> projectMemberships;
    private final String issuer;
    private final String audience;
    private final ZonedDateTime issuedDate;
    private final ZonedDateTime notBeforeDate;
    private final ZonedDateTime expirationDate;
    private final int refreshCount;
    private final int refreshLimit;

    private AuthenticationTokenDetails(String id, String username, String role, Map<String, List<String>> projectMemberships,
                                       String issuer, String audience,
                                       ZonedDateTime notBeforeDate, ZonedDateTime issuedDate, ZonedDateTime expirationDate,
                                       int refreshCount, int refreshLimit) {
        this.id = id;
        this.username = username;
        this.role = role;
        this.projectMemberships = unmodifiableCopy(projectMemberships);
        this.issuer = issuer;
        this.audience = audience;
        this.notBeforeDate = notBeforeDate;
        this.issuedDate = issuedDate;
        this.expirationDate = expirationDate;
        this.refreshCount = refreshCount;
        this.refreshLimit = refreshLimit;
    }

    /**
     * @return an unmodifiable copy of the given memberships, keeping the order of the projects and of their roles. Null
     * if the given memberships are null.
     */
    private static Map<String, List<String>> unmodifiableCopy(Map<String, List<String>> projectMemberships) {
        if (projectMemberships == null) {
            return null;
        }
        Map<String, List<String>> copy = new LinkedHashMap<>();
        projectMemberships.forEach((project, roles) -> copy.put(project, List.copyOf(roles)));
        return Collections.unmodifiableMap(copy);
    }

    public String getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public String getRole() {
        return role;
    }

    /**
     * @return for each project, the roles granted by the token, ordered from the highest to the lowest priority. Null
     * if the project memberships weren't extracted from the token: they must then not be synchronized. Empty if the
     * token grants no membership. The map and the lists are unmodifiable.
     */
    public Map<String, List<String>> getProjectMemberships() {
        return projectMemberships;
    }

    public String getIssuer() {
        return issuer;
    }

    public String getAudience() {
        return audience;
    }

    public ZonedDateTime getNotBeforeDate() {
        return notBeforeDate;
    }

    public ZonedDateTime getIssuedDate() {
        return issuedDate;
    }

    public ZonedDateTime getExpirationDate() {
        return expirationDate;
    }

    public int getRefreshCount() {
        return refreshCount;
    }

    public int getRefreshLimit() {
        return refreshLimit;
    }

    /**
     * Check if the authentication token is eligible for refreshment.
     *
     * @return
     */
    public boolean isEligibleForRefreshment() {
        return refreshCount < refreshLimit;
    }

    /**
     * Builder for the {@link AuthenticationTokenDetails}.
     */
    public static class Builder {

        private String id;
        private String username;
        private String role;
        private Map<String, List<String>> projectMemberships;
        private String issuer;
        private String audience;
        private ZonedDateTime notBeforeDate;
        private ZonedDateTime issuedDate;
        private ZonedDateTime expirationDate;
        private int refreshCount;
        private int refreshLimit;

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withRole(String role) {
            this.role = role;
            return this;
        }

        public Builder withProjectMemberships(Map<String, List<String>> projectMemberships) {
            this.projectMemberships = projectMemberships;
            return this;
        }

        public Builder withIssuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Builder withAudience(String audience) {
            this.audience = audience;
            return this;
        }


        public Builder withNotBeforeDate(ZonedDateTime notBeforeDate) {
            this.notBeforeDate = notBeforeDate;
            return this;
        }

        public Builder withIssuedDate(ZonedDateTime issuedDate) {
            this.issuedDate = issuedDate;
            return this;
        }

        public Builder withExpirationDate(ZonedDateTime expirationDate) {
            this.expirationDate = expirationDate;
            return this;
        }

        public Builder withRefreshCount(int refreshCount) {
            this.refreshCount = refreshCount;
            return this;
        }

        public Builder withRefreshLimit(int refreshLimit) {
            this.refreshLimit = refreshLimit;
            return this;
        }

        public AuthenticationTokenDetails build() {
            return new AuthenticationTokenDetails(id, username, role, projectMemberships, issuer, audience, notBeforeDate, issuedDate, expirationDate, refreshCount, refreshLimit);
        }

    }

    @Override
    public String toString() {
        return "AuthenticationTokenDetails{" +
            "id='" + id + '\'' +
            ", username='" + username + '\'' +
            ", role='" + role + '\'' +
            ", projectMemberships=" + projectMemberships +
            ", issuer='" + issuer + '\'' +
            ", audience='" + audience + '\'' +
            ", issuedDate=" + issuedDate +
            ", notBeforeDate=" + notBeforeDate +
            ", expirationDate=" + expirationDate +
            ", refreshCount=" + refreshCount +
            ", refreshLimit=" + refreshLimit +
            '}';
    }
}

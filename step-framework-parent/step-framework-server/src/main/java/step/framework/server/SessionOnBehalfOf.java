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

import step.core.accessors.AbstractUser;

public class SessionOnBehalfOf<U extends AbstractUser> extends Session<U> {

    public SessionOnBehalfOf(Session<U> originalSession, U onBehalfOf) {
        // copy the original session context (including TenantContext)
        for (String key : originalSession.getKeys()) {
            put(key, originalSession.get(key));
        }
        this.setUser(onBehalfOf);
    }

    @Override
    public boolean isAuthenticated() {
        return true;
    }

    @Override
    public String getToken() {
        throw new UnsupportedOperationException("'on behalf of token' is not available");
    }

}

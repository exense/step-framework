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
package step.core.objectenricher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Collections;

public final class ObjectAccessException extends Exception {

    public final List<ObjectAccessViolation> violations;
    
    @JsonCreator
    public ObjectAccessException(@JsonProperty("violations") List<ObjectAccessViolation> violations) {
        super(buildMessage(violations));
        this.violations = violations != null ? List.copyOf(violations) : Collections.emptyList();
    }
    
    public List<ObjectAccessViolation> getViolations() {
        return violations;
    }
    
    private static String buildMessage(List<ObjectAccessViolation> violations) {
        if (violations == null || violations.isEmpty()) {
            return "Access denied";
        }
        if (violations.size() == 1) {
            return violations.get(0).message;
        }
        return "Access denied by " + violations.size() + " access control rule(s)";
    }
}
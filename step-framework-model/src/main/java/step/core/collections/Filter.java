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
package step.core.collections;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import step.core.collections.filters.*;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(And.class),
        @JsonSubTypes.Type(Equals.class),
        @JsonSubTypes.Type(False.class),
        @JsonSubTypes.Type(Fulltext.class),
        @JsonSubTypes.Type(Gt.class),
        @JsonSubTypes.Type(Gte.class),
        @JsonSubTypes.Type(Lt.class),
        @JsonSubTypes.Type(Lte.class),
        @JsonSubTypes.Type(Not.class),
        @JsonSubTypes.Type(Or.class),
        @JsonSubTypes.Type(Regex.class),
        @JsonSubTypes.Type(True.class),
        @JsonSubTypes.Type(Exists.class)
})
public interface Filter {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    String getField();

    List<Filter> getChildren();
}

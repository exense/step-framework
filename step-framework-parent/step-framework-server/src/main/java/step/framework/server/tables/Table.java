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
package step.framework.server.tables;

import step.core.collections.Filter;
import step.core.collections.SearchOrder;
import step.core.objectenricher.ObjectFilter;
import step.core.objectenricher.ObjectHookRegistry;
import step.framework.server.tables.service.TableParameters;

import java.util.List;

public interface Table<T> {

    /**
     * @param columnName the name of the column (field)
     * @return the distinct values of the column
     */
    List<String> distinct(String columnName);

    /**
     * @param columnName the name of the column (field)
     * @param filter:    the query filter
     * @return the distinct values of the column
     */
    List<String> distinct(String columnName, Filter filter);

    TableFindResult<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit);

    /**
     * @param tableParameters some context parameters that might be required to generate the additional query fragments
     * @return a list of query fragments to be appended to the queries when calling the method find()
     */
    List<Filter> getTableFilters(TableParameters tableParameters);

    /**
     * @return true if the filter defined by the {@link ObjectFilter} of the {@link ObjectHookRegistry} have to be applied
     * when performing a search
     */
    boolean isContextFiltered();

}
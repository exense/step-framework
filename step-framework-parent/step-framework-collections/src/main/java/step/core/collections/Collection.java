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

import java.util.List;
import java.util.stream.Stream;

public interface Collection<T> {

	/**
	 * Counts the number of entities matching the provided filter
	 * 
	 * @param filter
	 * @param limit the maximum number of documents to count. Nullable.
	 * @return the number of entities matching the provided filter
	 */
	long count(Filter filter, Integer limit);
	
	/**
	 * Provide an estimation of the total number of entities of the collection
	 * 
	 * @return
	 */
	long estimatedCount();
	
	/**
	 * @param filter
	 * @param order
	 * @param skip
	 * @param limit
	 * @param maxTime
	 * @return
	 */
	Stream<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime);

	Stream<T> findReduced(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime, List<String> reduceFields);
	
	/**
	 * @param columnName the name of the column (field)
	 * @param filter: the query filter
	 * @return the distinct values of the column 
	 */
	List<String> distinct(String columnName, Filter filter);

	void remove(Filter filter);
	
	T save(T entity);
	
	void save(Iterable<T> entities);

	void createOrUpdateIndex(String field);

	void createOrUpdateCompoundIndex(String... fields);
	
	void rename(String newName);
	
	void drop();
}
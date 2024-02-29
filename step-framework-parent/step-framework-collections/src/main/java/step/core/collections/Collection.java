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

import java.util.LinkedHashSet;
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
	 * Warning: depending on the underlying implementation the whole result set might be loaded into memory
	 * Use {@link #findLazy(Filter, SearchOrder, Integer, Integer, int) FindLazy} method instead within a try-with-resources statement for large volume of data
	 * @param filter
	 * @param order
	 * @param skip
	 * @param limit
	 * @param maxTime
	 * @return
	 */
	Stream<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime);

	/**
	 * @param filter
	 * @param order
	 * @param skip
	 * @param limit
	 * @param maxTime
	 * @return the query results as a Stream
	 *
	 * API Note: this method must be used within a try-with-resources statement or similar control structure to ensure that the stream's I/O resources are closed promptly after the stream's operations have completed.
	 */

	Stream<T> findLazy(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime);

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

	void createOrUpdateIndex(IndexField indexField);

	void createOrUpdateIndex(String field, Order order);

	void createOrUpdateCompoundIndex(String... fields);

	void createOrUpdateCompoundIndex(LinkedHashSet<IndexField> fields);

	void rename(String newName);
	
	void drop();

	Class<T> getEntityClass();

	void dropIndex(String indexName);
}
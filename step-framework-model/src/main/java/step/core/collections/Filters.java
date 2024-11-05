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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;

import step.core.accessors.AbstractIdentifiableObject;
import step.core.collections.filters.*;

public class Filters {

	public interface FilterFactory<T> {
		
		T buildFilter(Filter filter);
	}
	
	public static And and(List<Filter> filters) {
		return new And(filters);
	}
	
	public static Or or(List<Filter> filters) {
		return new Or(filters);
	}
	
	public static Not not(Filter filter) {
		return new Not(filter);
	}
	
	public static True empty() {
		return new True();
	}

	public static False falseFilter() {
		return new False();
	}
	
	public static Equals equals(String field, boolean expectedValue) {
		return new Equals(field, expectedValue);
	}
	
	public static Equals equals(String field, long expectedValue) {
		return new Equals(field, expectedValue);
	}
	
	public static Equals equals(String field, String expectedValue) {
		return new Equals(field, expectedValue);
	}
	
	public static Equals equals(String field, ObjectId expectedValue) {
		return new Equals(field, expectedValue);
	}
	
	public static Equals id(ObjectId id) {
		return equals(AbstractIdentifiableObject.ID, id);
	}
	
	public static Equals id(String id) {
		return id(new ObjectId(id));
	}
	
	public static Lt lt(String field, long value) {
		return new Lt(field, value);
	}
	
	public static Lte lte(String field, long value) {
		return new Lte(field, value);
	}
	
	public static Gt gt(String field, long value) {
		return new Gt(field, value);
	}
	
	public static Gte gte(String field, long value) {
		return new Gte(field, value);
	}
	
	public static Filter in(String field, List<String> values) {
		List<Filter> filters = new ArrayList<>();
		for (String v : values) {
			filters.add(Filters.equals(field, v));
		}
		return (filters.isEmpty()) ? Filters.falseFilter() : Filters.or(filters);
	}
	
	public static Regex regex(String field, String expression, boolean caseSensitive) {
		return new Regex(field, expression, caseSensitive);
	}
	
	public static Fulltext fulltext(String expression) {
		return new Fulltext(expression);
	}

	public static Exists exists(String field) {
		return new Exists(field);
	}

	public static Set<String> collectFilterAttributes(Filter filter) {
		Set<String> collectedAttributes = new HashSet<>();
		collectFilterAttributesRecursively(filter, collectedAttributes);
		return  collectedAttributes;
	}

	public static void collectFilterAttributesRecursively(Filter filter, Set<String> collectedAttributes) {
		if (filter.getField() != null) {
			collectedAttributes.add(filter.getField());
		}
		if (filter.getChildren() != null) {
			filter.getChildren().forEach(c -> collectFilterAttributesRecursively(c, collectedAttributes));
		}
	}
}

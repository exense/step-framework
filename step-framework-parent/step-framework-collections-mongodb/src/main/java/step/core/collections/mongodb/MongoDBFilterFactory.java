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
package step.core.collections.mongodb;

import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import step.core.collections.Filter;
import step.core.collections.Filters.FilterFactory;
import step.core.collections.filters.*;

import java.util.List;
import java.util.stream.Collectors;

public class MongoDBFilterFactory implements FilterFactory<Bson> {

	@Override
	public Bson buildFilter(Filter filter) {
		List<Bson> childerPojoFilters;
		List<Filter> children = filter.getChildren();
		if(children != null) {
			childerPojoFilters = filter.getChildren().stream().map(this::buildFilter)
					.collect(Collectors.toList());
		} else {
			childerPojoFilters = null;
		}

		if (filter instanceof And) {
			return com.mongodb.client.model.Filters.and(childerPojoFilters);
		} else if (filter instanceof Or) {
			return com.mongodb.client.model.Filters.or(childerPojoFilters);
		} else if (filter instanceof Not) {
			return com.mongodb.client.model.Filters.not(childerPojoFilters.get(0));
		} else if (filter instanceof True) {
			return com.mongodb.client.model.Filters.expr(true);
		} else if (filter instanceof False) {
			return com.mongodb.client.model.Filters.expr(false);
		} else if (filter instanceof Equals) {
			Equals equalsFilter = (Equals) filter;
			String field = equalsFilter.getField();
			Object expectedValue = equalsFilter.getExpectedValue();
			if(field.equals("id")) {
				field = "_id";
				if (expectedValue instanceof String) {
					expectedValue = new ObjectId((String) expectedValue);
				}
			}
			return com.mongodb.client.model.Filters.eq(field, expectedValue);
		} else if (filter instanceof Regex) {
			Regex regexFilter = (Regex) filter;
			if(regexFilter.isCaseSensitive()) {
				return com.mongodb.client.model.Filters.regex(regexFilter.getField(), regexFilter.getExpression());
			} else {
				return com.mongodb.client.model.Filters.regex(regexFilter.getField(), regexFilter.getExpression(), "i");
			}
		} else if (filter instanceof Gt) {
			Gt gtFilter = (Gt) filter;
			return com.mongodb.client.model.Filters.gt(gtFilter.getField(), gtFilter.getValue());
		} else if (filter instanceof Gte) {
			Gte gteFilter = (Gte) filter;
			return com.mongodb.client.model.Filters.gte(gteFilter.getField(), gteFilter.getValue());
		} else if (filter instanceof Lt) {
			Lt ltFilter = (Lt) filter;
			return com.mongodb.client.model.Filters.lt(ltFilter.getField(), ltFilter.getValue());
		} else if (filter instanceof Lte) {
			Lte lteFilter = (Lte) filter;
			return com.mongodb.client.model.Filters.lte(lteFilter.getField(), lteFilter.getValue());
		} else if (filter instanceof Exists) {
			Exists existsFilter = (Exists) filter;
			return com.mongodb.client.model.Filters.exists(existsFilter.getField());
		} else {
			throw new IllegalArgumentException("Unsupported filter type " + filter.getClass());
		}
	}
}

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
package step.core.collections.postgresql;

import org.bson.types.ObjectId;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.filters.*;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PostgreSQLFilterFactory implements Filters.FilterFactory<String> {

	private static final Pattern p = Pattern.compile("([^.]+)");
	@Override
	public String buildFilter(Filter filter) {
		List<String> childerPojoFilters;
		List<Filter> children = filter.getChildren();
		if(children != null) {
			childerPojoFilters = filter.getChildren().stream().map(this::buildFilter)
					.collect(Collectors.toList());
		} else {
			childerPojoFilters = null;
		}

		if (filter instanceof And) {
			return subQueryWithChildren(childerPojoFilters, "AND");
		} else if (filter instanceof Or) {
			return subQueryWithChildren(childerPojoFilters, "OR");
		} else if (filter instanceof Not) {
			return "NOT (" + childerPojoFilters.get(0) + ")";
		} else if (filter instanceof True) {
			return "TRUE";
		} else if (filter instanceof False) {
			return "FALSE";
		} else if (filter instanceof Equals) {
			Equals equalsFilter = (Equals) filter;
			Object expectedValue = equalsFilter.getExpectedValue();
			String formattedFieldName = formatField(equalsFilter.getField(),
					(expectedValue!=null) ? expectedValue.getClass(): String.class);
			if (expectedValue == null) {
				return formattedFieldName + " IS NULL";
			} else {
				if (expectedValue instanceof ObjectId) {
					expectedValue = ((ObjectId) expectedValue).toHexString();
				}
				return formattedFieldName + " = '" + expectedValue + "'";
			}
		} else if (filter instanceof Regex) {
			Regex regexFilter = (Regex) filter;
			String operator = (regexFilter.isCaseSensitive()) ? " ~ " : " ~* ";
			return formatFieldForValueAsText(regexFilter.getField()) + operator + "'" + regexFilter.getExpression() + "'";
		} else if (filter instanceof Gt) {
			Gt gtFilter = (Gt) filter;
			return formatField(gtFilter.getField(),true) + " IS NOT NULL AND "
					+ formatField(gtFilter.getField(),false) + " > '" + gtFilter.getValue() + "'";
		} else if (filter instanceof Gte) {
			Gte gteFilter = (Gte) filter;
			return formatField(gteFilter.getField(),true) + " IS NOT NULL AND "
					+ formatField(gteFilter.getField(),false) + " >= '" + gteFilter.getValue() + "'";
		} else if (filter instanceof Lt) {
			Lt ltFilter = (Lt) filter;
			return formatField(ltFilter.getField(),true) + " IS NOT NULL AND "
					+ formatField(ltFilter.getField(),false) + " < '" + ltFilter.getValue() + "'";
		} else if (filter instanceof Lte) {
			Lte lteFilter = (Lte) filter;
			return formatField(lteFilter.getField(),true) + " IS NOT NULL AND "
					+ formatField(lteFilter.getField(),false) + " <= '" + lteFilter.getValue() + "'";
		} else {
			throw new IllegalArgumentException("Unsupported filter type " + filter.getClass());
		}
	}

	public static String formatField(String field, Class<?> clazz) {
		return formatField(field, clazz.equals(String.class) || clazz.equals(ObjectId.class));
	}

	public static String formatFieldForValueAsText(String field) {
		return formatField(field, true);
	}

	private static String formatField(String field, boolean asText) {
		if (field.equals(AbstractIdentifiableObject.ID)) {
			return field;
		}
		StringBuilder b = new StringBuilder();
		b.append("object");
		Matcher m = p.matcher(field);
		String previous = null;
		while (m.find()) {
			if (previous != null) {
				b.append("->'").append(previous).append("'");
			}
			previous = m.group(1);
		}
		if (previous == null) {
			throw new RuntimeException("Failed to format jsonb field: " + field);
		}
		if (asText) {
			b.append("->>'");
		} else {
			b.append("->'");
		}
		b.append(previous).append("'");
		return b.toString();
	}

	private String subQueryWithChildren(List<String> childerPojoFilters, String operator) {
		if (childerPojoFilters == null || childerPojoFilters.isEmpty()) {
			return "";
		} else if (childerPojoFilters.size() == 1) {
			return childerPojoFilters.get(0);
		} else {
			StringBuilder buf = new StringBuilder();
			buf.append("(").append(childerPojoFilters.get(0));
			for (int i=1; i < childerPojoFilters.size(); i++) {
				buf.append(" ").append(operator).append(" ").append(childerPojoFilters.get(i));
			}
			buf.append(")");
			return buf.toString();
		}
	}

}

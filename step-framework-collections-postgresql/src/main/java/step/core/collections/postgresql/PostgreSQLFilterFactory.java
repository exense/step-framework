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

import org.apache.commons.beanutils.PropertyUtils;
import org.bson.types.ObjectId;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.filters.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PostgreSQLFilterFactory implements Filters.FilterFactory<String> {

	private static final Pattern p = Pattern.compile("([^.]+)");
	private static final Map<String, Class<?>> fieldToClassCache = new ConcurrentHashMap<>();

	public final Class<?> entityClass;

    public PostgreSQLFilterFactory(Class<?> entityClass) {
        this.entityClass = entityClass;
    }

    @Override
	public String buildFilter(Filter filter) {
		List<String> childrenPojoFilters;
		List<Filter> children = filter.getChildren();
		if(children != null) {
			childrenPojoFilters = filter.getChildren().stream().map(this::buildFilter)
					.collect(Collectors.toList());
		} else {
			childrenPojoFilters = null;
		}

		if (filter instanceof And) {
			return subQueryWithChildren(childrenPojoFilters, "AND");
		} else if (filter instanceof Or) {
			return subQueryWithChildren(childrenPojoFilters, "OR");
		} else if (filter instanceof Not) {
			return notPsqlClause((Not) filter, childrenPojoFilters);
		} else if (filter instanceof True) {
			return "TRUE";
		} else if (filter instanceof False) {
			return "FALSE";
		} else if (filter instanceof Equals) {
			Equals equalsFilter = (Equals) filter;
			Object expectedValue = equalsFilter.getExpectedValue();
			if (expectedValue == null) {
				//We cannot infer the type from null value, and index can only be used if we cast to the field type
				return formatFieldForEntity(equalsFilter.getField()) + " IS NULL";
			} else {
				String formattedFieldName = formatField(equalsFilter.getField(), expectedValue.getClass());
				String filterValue;
				if (expectedValue instanceof ObjectId) {
					filterValue = ((ObjectId) expectedValue).toHexString();
				} else if (expectedValue instanceof String) {
					filterValue = escapeValue((String) expectedValue);
				} else {
					filterValue = expectedValue.toString();
				}
				if (useCasting(formattedFieldName)) {
					return formattedFieldName + " = " + filterValue;
				} else {
					return formattedFieldName + " = '" + filterValue + "'";
				}

			}
		} else if (filter instanceof Regex) {
			Regex regexFilter = (Regex) filter;
			String operator = (regexFilter.isCaseSensitive()) ? " ~ " : " ~* ";
			return formatFieldForValueAsText(regexFilter.getField()) + operator + "'" + escapeValue(regexFilter.getExpression()) + "'";
		} else if (filter instanceof Gt) {
			Gt gtFilter = (Gt) filter;
			Class<?> valueType = gtFilter.getValueType();
			return formatField(gtFilter.getField(), valueType) + " IS NOT NULL AND "
					+ formatField(gtFilter.getField(), valueType) + " > " + gtFilter.getValue();
		} else if (filter instanceof Gte) {
			Gte gteFilter = (Gte) filter;
			Class<?> valueType = gteFilter.getValueType();
			return formatField(gteFilter.getField(),valueType) + " IS NOT NULL AND "
					+ formatField(gteFilter.getField(),valueType) + " >= " + gteFilter.getValue();
		} else if (filter instanceof Lt) {
			Lt ltFilter = (Lt) filter;
			Class<?> valueType = ltFilter.getValueType();
			return formatField(ltFilter.getField(),valueType) + " IS NOT NULL AND "
					+ formatField(ltFilter.getField(),valueType) + " < " + ltFilter.getValue();
		} else if (filter instanceof Lte) {
			Lte lteFilter = (Lte) filter;
			Class<?> valueType = lteFilter.getValueType();
			return formatField(lteFilter.getField(),valueType) + " IS NOT NULL AND "
					+ formatField(lteFilter.getField(),valueType) + " <= " + lteFilter.getValue();
		} else if (filter instanceof Exists) {
			Exists existsFilter = (Exists) filter;
			// For exists filter we have no value to infer the type, so we still try to infer the type from the field type
			return formatFieldForEntity(existsFilter.getField()) + " IS NOT NULL ";
		} else if (filter instanceof In) {
            In inFilter = (In) filter;
            //In filter values implementation only support comparison as Strings, so the field and values are formated to string
            String values = inFilter.getValues().stream()
                    .map(this::formatInValue) // Escape single quotes for SQL
                    .collect(Collectors.joining(",", "(", ")"));
            return formatField(inFilter.getField(), true) + " IN " + values + " ";
        } else {
            throw new IllegalArgumentException("Unsupported filter type " + filter.getClass());
        }
	}

	private String notPsqlClause(Not notFilter, List<String> childerPojoFilters) {
		//For psql filering out with NOT field = value will also filter out field is null
		if (notFilter.getChildren().get(0) instanceof Equals && ((Equals) notFilter.getChildren().get(0)).getExpectedValue() != null) {
			Equals eqFilter = (Equals) notFilter.getChildren().get(0);
			return "(NOT (" + childerPojoFilters.get(0) + ") OR " + buildFilter(Filters.equals(eqFilter.getField(), (String) null)) + ")";
		} else {
			return "NOT (" + childerPojoFilters.get(0) + ")";
		}
	}

    private String formatInValue(Object expectedValue) {
        String result;
        if (expectedValue instanceof String) {
            result = escapeValue((String) expectedValue);
        } else if (expectedValue instanceof ObjectId) {
            result = ((ObjectId) expectedValue).toHexString();
        } else {
            result = expectedValue.toString();
        }
        return "'" + result + "'";
    }

	private String escapeValue(String expectedValue) {
		return expectedValue.replaceAll("'","''");
	}

	private String formatFieldForEntity(String field) {
		return formatField(field, getFieldClassOfEntity(field, entityClass));
	}

	public static String formatFieldForValueAsText(String field) {
		return formatField(field, String.class);
	}

	public static String formatField(String fieldPath, Class<?> expectedType) {
		//Query on the ID column directly
		if (fieldPath.equals(AbstractIdentifiableObject.ID)) {
			return fieldPath;
		}
		StringBuilder formattedFieldBuilder = new StringBuilder();
		boolean useCasting = false;
		//Use safe casting function for numbers and boolean
		// Check for Boolean
		if (expectedType == Boolean.class || expectedType == boolean.class) {
			formattedFieldBuilder.append("safe_cast_to_bool(");
			useCasting = true;
		}
		// Check for any Numeric type (Long, Integer, Float, Double, etc.)
		// Number.class.isAssignableFrom handles the wrapper classes.
		// The primitive checks cover the basic types.
		else if (Number.class.isAssignableFrom(expectedType) ||
				expectedType == long.class || expectedType == int.class ||
				expectedType == double.class || expectedType == float.class) {
			formattedFieldBuilder.append("safe_cast_to_numeric(");
			useCasting = true;
		}


		//The root always start with the generic jsonb column name 'object'
		formattedFieldBuilder.append("object");
		Matcher m = p.matcher(fieldPath);
		String previousField = null;
		//the pattern is used to split the JSON path into individual fields (ex: for root.attributes.attr1 root -> attributes -> attr1)
		//The loop extract the current field, and add the previousField to the builder, the final field (leaf) is formatted and added after the loop
		while (m.find()) {
			if (previousField != null) {
				formattedFieldBuilder.append("->'").append(previousField).append("'");
			}
			previousField = m.group(1);
			if (previousField.equals("_id")) {
				previousField = "id";
			}
		}
		if (previousField == null) {
			throw new RuntimeException("Failed to format jsonb fieldPath: " + fieldPath);
		}

		if (useCasting) {
			formattedFieldBuilder.append("->'").append(previousField).append("')");
		} else {
			//Fall back to string for all other cases
			formattedFieldBuilder.append("->>'").append(previousField).append("'");
		}
		return formattedFieldBuilder.toString();
	}

	public static boolean useCasting(String formattedField) {
		return formattedField.contains("safe_cast");
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

	/**
	 * Recursively find the field passed in argument in the entityClass of this Collection and return its class
	 * This method is used to determine whether this field should be treated as text or object (required when building indexes,
	 * for distinct queries and for order by clause)
	 * Note that it only supports a subset of class fields, only map parametrized type are supported
	 * @param field the name of the field to be found
	 * @return the Class of the field
	 */
	protected static Class<?> getFieldClassOfEntity(String field, Class<?> entityClass)  {
		return fieldToClassCache.computeIfAbsent(entityClass.getSimpleName()+field, (k) -> {
			Matcher m = p.matcher(field);
			String previous = null;
			Class<?> currentClass = entityClass;
			while (m.find()) {
				if (previous != null) {
					currentClass = getFieldClass(currentClass, previous);
				}
				previous = m.group(1);
			}
			if (previous == null) {
				throw new RuntimeException("Failed to format jsonb field: " + field);
			}
			return getFieldClass(currentClass, previous);
		});
	}

	protected static Class<?> getFieldClass(Class<?> clazz, String _field) {
		String fieldName = _field.equals("_id") ? "id" : _field;
		if (fieldName.equals("_class")) {
			return String.class;
		}
		//Try with getters
		for (PropertyDescriptor propertyDescriptor: PropertyUtils.getPropertyDescriptors(clazz)) {
			if (propertyDescriptor.getName().equals(fieldName)) {
				Type genericReturnType = propertyDescriptor.getReadMethod().getGenericReturnType();
				return getFieldClassForActualField(clazz, fieldName, genericReturnType);
			}
		}
		//try with public fields
		for (Field field : clazz.getFields()) {
			//getFields only return public fields, but the check is cheap, so keeping it for safety and clarity
			if (Modifier.isPublic(field.getModifiers()) && fieldName.equals(field.getName())) {
				Type genericType = field.getGenericType();
				return getFieldClassForActualField(clazz, fieldName, genericType);
			}
		}
		//current property was not found, this happens for nested fields in Maps
		return clazz;
	}

	protected static Class<?> getFieldClassForActualField(Class<?> clazz, String fieldName, Type genericType) {
		if (genericType instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) genericType;
			Class<?> rawType = (Class<?>) pt.getRawType();

			if (Map.class.isAssignableFrom(rawType) && pt.getActualTypeArguments().length == 2) {
				// Get the value type of the Map
				Type valueType = pt.getActualTypeArguments()[1];
				if (valueType instanceof Class<?>) {
					return (Class<?>) valueType;
				} else if (valueType instanceof ParameterizedType) {
					return (Class<?>) ((ParameterizedType) valueType).getRawType();
				} else {
					throw new UnsupportedOperationException("Unable to retrieve type of ParameterizedType field '" + fieldName + "' for class '" + clazz.getSimpleName());
				}
			} else {
				throw new UnsupportedOperationException("Unable to retrieve type of ParameterizedType field '" +
						fieldName + "' for class '" + clazz.getSimpleName() + "'");
			}
		} else if (genericType instanceof TypeVariable) {
			// Handle generic type variables (e.g., T)
			Type[] bounds = ((TypeVariable<?>) genericType).getBounds();
			if (bounds.length == 0) {
				throw  new RuntimeException("Reflection failed for clazz '" + clazz + "' and field '" + fieldName + "'" );
			}
			return (Class<?>) bounds[0];
		} else if (genericType instanceof Class<?>) {
			Class<?> fieldClass = (Class<?>) genericType;
			return (fieldClass.isEnum()) ? String.class : fieldClass;
		} else {
			throw new UnsupportedOperationException("Unsupported generic type: " + genericType.getTypeName());
		}
	}

}

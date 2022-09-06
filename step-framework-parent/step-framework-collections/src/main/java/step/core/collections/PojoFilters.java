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

import org.bson.types.ObjectId;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.collections.Filters.FilterFactory;
import step.core.collections.filters.*;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PojoFilters {

	public static class PojoFilterFactory<POJO> implements FilterFactory<PojoFilter<?>> {

		@Override
		public PojoFilter<POJO> buildFilter(Filter filter) {

			List<PojoFilter<POJO>> childerPojoFilters;
			List<Filter> children = filter.getChildren();
			if(children != null) {
				childerPojoFilters = children.stream().map(this::buildFilter)
						.collect(Collectors.toList());
			} else {
				childerPojoFilters = null;
			}

			if (filter instanceof And) {
				return new AndPojoFilter<>(childerPojoFilters);
			} else if (filter instanceof Or) {
				return new OrPojoFilter<>(childerPojoFilters);
			} else if (filter instanceof Not) {
				return new NotPojoFilter<>(childerPojoFilters.get(0));
			} else if (filter instanceof Equals) {
				return new EqualsPojoFilter<>((Equals) filter);
			} else if (filter instanceof Regex) {
				return new RegexPojoFilter<>((Regex) filter);
			} else if (filter instanceof True) {
				return new TruePojoFilter<>();
			}  else if (filter instanceof False) {
				return new FalsePojoFilter<>();
			} else if (filter instanceof Lt) {
				return new LtPojoFilter<>((Lt) filter);
			} else if (filter instanceof Lte) {
				return new LtePojoFilter<>((Lte) filter);
			} else if (filter instanceof Gt) {
				return new GtPojoFilter<>((Gt) filter);
			}else if (filter instanceof Gte) {
				return new GtePojoFilter<>((Gte) filter);
			} else {
				throw new IllegalArgumentException("Unsupported filter type " + filter.getClass());
			}
		}
	}

	public static class AndPojoFilter<T> implements PojoFilter<T> {

		private final List<PojoFilter<T>> pojoFilters;

		public AndPojoFilter(List<PojoFilter<T>> PojoFilters) {
			super();
			this.pojoFilters = PojoFilters;
		}

		@Override
		public boolean test(T t) {
			return pojoFilters.stream().allMatch(PojoFilter -> PojoFilter.test(t));
		}
	}

	public static class OrPojoFilter<T> implements PojoFilter<T> {

		private final List<PojoFilter<T>> pojoFilters;

		public OrPojoFilter(List<PojoFilter<T>> PojoFilters) {
			super();
			this.pojoFilters = PojoFilters;
		}

		@Override
		public boolean test(T t) {
			return pojoFilters.stream().anyMatch(PojoFilter -> PojoFilter.test(t));
		}
	}

	public static class NotPojoFilter<T> implements PojoFilter<T> {

		private final PojoFilter<T> pojoFilter;

		public NotPojoFilter(PojoFilter<T> PojoFilter) {
			super();
			this.pojoFilter = PojoFilter;
		}

		@Override
		public boolean test(T t) {
			return !pojoFilter.test(t);
		}
	}
	
	public static class TruePojoFilter<T> implements PojoFilter<T> {

		public TruePojoFilter() {
			super();
		}

		@Override
		public boolean test(T t) {
			return true;
		}
	}

	public static class FalsePojoFilter<T> implements PojoFilter<T> {

		public FalsePojoFilter() {
			super();
		}

		@Override
		public boolean test(T t) {
			return false;
		}
	}

	public static class EqualsPojoFilter<T> implements PojoFilter<T> {

		private final Equals equalsFilter;
		private final Object expectedValue;

		public EqualsPojoFilter(Equals equalsFilter) {
			super();
			this.equalsFilter = equalsFilter;
			String field = equalsFilter.getField();
			Object expectedValue = equalsFilter.getExpectedValue();
			if (field.equals(AbstractIdentifiableObject.ID) && expectedValue instanceof String) {
				this.expectedValue = new ObjectId((String) expectedValue);
			} else {
				this.expectedValue = expectedValue;
			}
		}

		@Override
		public boolean test(T t) {
			try {
				String field = equalsFilter.getField();
				Object beanProperty = getBeanProperty(t, field);
				if(expectedValue != null) {
					if(expectedValue instanceof Number) {
						if(beanProperty != null) {
							return new BigDecimal(expectedValue.toString()).compareTo(new BigDecimal(beanProperty.toString()))==0;
						} else {
							return false;
						}
					} if (expectedValue instanceof String && beanProperty!= null && beanProperty.getClass().isEnum())  {
						return expectedValue.equals(beanProperty.toString());
					} else {
						return expectedValue.equals(beanProperty);
					}
				} else {
					return beanProperty == null; 
				}
			} catch (NoSuchMethodException e) {
				return (expectedValue == null);
			} catch (IllegalAccessException | InvocationTargetException e) {
				return false;
			}
		}
	}

	public static class RegexPojoFilter<T> implements PojoFilter<T> {

		private final Regex regexFilter;
		private final Pattern pattern;

		public RegexPojoFilter(Regex regexFilter) {
			super();
			this.regexFilter = regexFilter;
			String expression = "";
			if(!regexFilter.isCaseSensitive()) {
				expression += "(?i)";
			}
			expression += regexFilter.getExpression();
			pattern = Pattern.compile(expression);
		}

		@Override
		public boolean test(T t) {
			try {
				Object beanProperty = getBeanProperty(t, regexFilter.getField());
				if(beanProperty != null) {
					Matcher matcher = pattern.matcher(beanProperty.toString());
					return matcher.find();
				} else {
					return false;
				}
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				return false;
			}
		}
	}

	public static class LtPojoFilter<T> implements PojoFilter<T> {

		private final Lt ltFilter;

		public LtPojoFilter(Lt ltFilter) {
			super();
			this.ltFilter = ltFilter;
		}

		@Override
		public boolean test(T t) {
			try {
				String field = ltFilter.getField();
				Object beanProperty = getBeanProperty(t, field);
				long value = ltFilter.getValue();
				if(beanProperty instanceof Number) {
					Number fieldValue = (Number) beanProperty;
					return ( fieldValue.longValue() < value);
				} else {
					throw new RuntimeException("Gt,Gte,Lt and Lte filters only support numbers, provided field is not compatible: " + field);
				}
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				return false;
			}
		}
	}

	public static class LtePojoFilter<T> implements PojoFilter<T> {

		private final Lte lteFilter;

		public LtePojoFilter(Lte lteFilter) {
			super();
			this.lteFilter = lteFilter;
		}

		@Override
		public boolean test(T t) {
			try {
				String field = lteFilter.getField();
				Object beanProperty = getBeanProperty(t, field);
				long value = lteFilter.getValue();
				if(beanProperty instanceof Number) {
					Number fieldValue = (Number) beanProperty;
					return ( fieldValue.longValue() <= value);
				} else {
					throw new RuntimeException("Gt,Gte,Lt and Lte filters only support numbers, provided field is not compatible: " + field);
				}
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				return false;
			}
		}
	}

	public static class GtPojoFilter<T> implements PojoFilter<T> {

		private final Gt gtFilter;

		public GtPojoFilter(Gt gtFilter) {
			super();
			this.gtFilter = gtFilter;
		}

		@Override
		public boolean test(T t) {
			try {
				String field = gtFilter.getField();
				Object beanProperty = getBeanProperty(t, field);
				long value = gtFilter.getValue();
				if(beanProperty instanceof Number) {
					Number fieldValue = (Number) beanProperty;
					return ( fieldValue.longValue() > value);
				} else {
					throw new RuntimeException("Gt,Gte,Lt and Lte filters only support numbers, provided field is not compatible: " + field);
				}
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				return false;
			}
		}
	}

	public static class GtePojoFilter<T> implements PojoFilter<T> {

		private final Gte gteFilter;

		public GtePojoFilter(Gte gteFilter) {
			super();
			this.gteFilter = gteFilter;
		}

		@Override
		public boolean test(T t) {
			try {
				String field = gteFilter.getField();
				Object beanProperty = getBeanProperty(t, field);
				long value = gteFilter.getValue();
				if (beanProperty instanceof Number) {
					Number fieldValue = (Number) beanProperty;
					return (fieldValue.longValue() >= value);
				} else {
					throw new RuntimeException("Gt,Gte,Lt and Lte filters only support numbers, provided field is not compatible: " + field);
				}
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				return false;
			}
		}
	}
	
	private static Object getBeanProperty(Object t, String fieldName)
			throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		return PojoUtils.getProperty(t, fieldName);
	}
}

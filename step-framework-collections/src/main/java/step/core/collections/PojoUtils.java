package step.core.collections;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.NestedNullException;
import org.apache.commons.beanutils.PropertyUtilsBean;
import step.core.accessors.AbstractIdentifiableObject;


public class PojoUtils {

    private static BeanUtilsBean beanUtilsBean;
    private static final Map<Class<?>, Optional<TypeIdResolver>> typeIdResolvers = new ConcurrentHashMap<>();

    public static Object getProperty(Object bean, String propertyName) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        try {
            return beanUtilsBean.getPropertyUtils().getNestedProperty(bean, propertyName);
        } catch (NestedNullException e) {
            return null;
        } catch (NoSuchMethodException noSuchMethod) {
            try {
                // fallback, try to get the field with the given name
                return getField(bean, propertyName);
            } catch (NoSuchFieldException ignored) {
                // keep existing behavior - no such method, and no field either,
                // so we just throw the same exception we always did
                throw noSuchMethod;
            }
        }
    }

    private static Object getField(Object bean, String propertyName) throws NoSuchFieldException, IllegalAccessException {
        return bean == null ? null : bean.getClass().getField(propertyName).get(bean);
    }

    /**
     * @return the type id of the bean as serialized to the "_class" property: the id given by the
     * {@link JsonTypeIdResolver} of the bean if it declares one, its class name otherwise
     */
    private static String getTypeId(Object bean) {
        return typeIdResolvers.computeIfAbsent(bean.getClass(), PojoUtils::createTypeIdResolver)
            .map(resolver -> resolver.idFromValue(bean))
            .orElseGet(() -> bean.getClass().getName());
    }

    private static Optional<TypeIdResolver> createTypeIdResolver(Class<?> beanClass) {
        for (Class<?> c = beanClass; c != null; c = c.getSuperclass()) {
            JsonTypeIdResolver annotation = c.getAnnotation(JsonTypeIdResolver.class);
            if (annotation != null) {
                try {
                    TypeIdResolver resolver = annotation.value().getDeclaredConstructor().newInstance();
                    resolver.init(TypeFactory.defaultInstance().constructType(c));
                    return Optional.of(resolver);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("Unable to create the type id resolver of " + beanClass.getName(), e);
                }
            }
        }
        return Optional.empty();
    }

    public static class SearchOrderComparator<T> implements Comparator<T> {

        List<SearchOrder.FieldSearchOrder> fieldsSearchOrder;

        public SearchOrderComparator(List<SearchOrder.FieldSearchOrder> fieldsSearchOrder) {
            this.fieldsSearchOrder = fieldsSearchOrder;
        }

        private String extractValueAsString(T o, String attributeName) {
            try {
                return getProperty(o, attributeName).toString();
            } catch (NoSuchMethodException e1) {
                return "";
            } catch (IllegalAccessException | InvocationTargetException e1) {
                throw new RuntimeException(e1);
            }
        }

        @Override
        public int compare(T o1, T o2) {
            try {
                for (SearchOrder.FieldSearchOrder fieldSearchOrder : fieldsSearchOrder) {
                    String attributeName = fieldSearchOrder.attributeName;
                    String value1 = extractValueAsString(o1, attributeName);
                    String value2 = extractValueAsString(o2, attributeName);

                    int comparison = value1.compareTo(value2);

                    if (comparison != 0) {
                        return fieldSearchOrder.order >= 0 ? comparison : -comparison;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Sorting error: " + e.getMessage(), e);
            }
            return 0;
        }
    }

    static {
        beanUtilsBean = new BeanUtilsBean(new ConvertUtilsBean(), new PropertyUtilsBean() {

            @Override
            public Object getSimpleProperty(Object bean, String name)
                throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
                if (name.equals("_class")) {
                    return getTypeId(bean);
                } else if (name.equals("_id")) {
                    return super.getSimpleProperty(bean, "id");
                } else {
                    return super.getSimpleProperty(bean, name);
                }
            }

            @Override
            protected Object getPropertyOfMapBean(Map<?, ?> bean, String propertyName) throws IllegalArgumentException,
                IllegalAccessException, InvocationTargetException, NoSuchMethodException {
                if (propertyName.equals(AbstractIdentifiableObject.ID)) {
                    if (bean instanceof Document) {
                        return ((Document) bean).getId();
                    } else {
                        return super.getPropertyOfMapBean(bean, propertyName);
                    }
                } else {
                    return super.getPropertyOfMapBean(bean, propertyName);
                }
            }
        });
    }
}

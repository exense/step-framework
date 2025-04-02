package step.core.collections;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.NestedNullException;
import org.apache.commons.beanutils.PropertyUtilsBean;
import step.core.accessors.AbstractIdentifiableObject;


public class PojoUtils {

    private static BeanUtilsBean beanUtilsBean;

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

    public static class SearchOrderComparator<T> implements Comparator<T> {

        List<SearchOrder.FieldSearchOrder> fieldsSearchOrder;

        public SearchOrderComparator(List<SearchOrder.FieldSearchOrder> fieldsSearchOrder) {
            this.fieldsSearchOrder = fieldsSearchOrder;
        }

        private String extractValueAsString(T o, String attributeName) {
            String value = "";
            try {
                value = getProperty(o, attributeName).toString();
            } catch (NoSuchMethodException e1) {
                //keep default value of
            } catch (IllegalAccessException | InvocationTargetException e1) {
                throw new RuntimeException(e1);
            }
            return value;
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
                    return bean.getClass().getName();
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

package step.core.collections;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
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

    public static Comparator<Object> comparator(String propertyName) {
        Comparator<Object> comparing = Comparator.comparing(e->{
            try {
                return getProperty(e, propertyName).toString();
            } catch (NoSuchMethodException e1) {
                return "";
            } catch (IllegalAccessException | InvocationTargetException e1) {
                throw new RuntimeException(e1);
            }
        });
        return comparing;
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

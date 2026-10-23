package step.core.collections;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import org.junit.Test;
import step.core.collections.PojoFilters.PojoFilterFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PojoUtilsTest {

    @Test
    public void testClassOfBeanWithoutTypeIdResolver() throws Exception {
        assertEquals(PlainBean.class.getName(), PojoUtils.getProperty(new PlainBean(), "_class"));
    }

    @Test
    public void testClassOfBeanWithTypeIdResolverOnSuperclass() throws Exception {
        assertEquals("SubBean", PojoUtils.getProperty(new SubBean(), "_class"));
        // The subclass of an annotated class
        assertEquals("SubSubBean", PojoUtils.getProperty(new SubSubBean(), "_class"));
    }

    @Test
    public void testClassOfBeanWithTypeIdResolverOnInterface() throws Exception {
        assertEquals("InterfaceBean", PojoUtils.getProperty(new InterfaceBean(), "_class"));
        // The interface is implemented by a superclass
        assertEquals("SubInterfaceBean", PojoUtils.getProperty(new SubInterfaceBean(), "_class"));
        // The annotated interface is extended by the implemented one
        assertEquals("SubInterfaceImplBean", PojoUtils.getProperty(new SubInterfaceImplBean(), "_class"));
    }

    @Test
    public void testNestedClass() throws Exception {
        Holder holder = new Holder();
        holder.setNested(new SubBean());
        assertEquals("SubBean", PojoUtils.getProperty(holder, "nested._class"));
        assertEquals(Holder.class.getName(), PojoUtils.getProperty(holder, "_class"));
    }

    @Test
    public void testFilterOnClass() {
        Holder holder = new Holder();
        holder.setNested(new InterfaceBean());
        PojoFilterFactory<Holder> filterFactory = new PojoFilterFactory<>();
        assertTrue(filterFactory.buildFilter(Filters.equals("nested._class", "InterfaceBean")).test(holder));
        assertFalse(filterFactory.buildFilter(Filters.equals("nested._class", InterfaceBean.class.getName())).test(holder));
    }

    /**
     * Identifies the types by their simple name
     */
    public static class SimpleNameTypeIdResolver extends TypeIdResolverBase {

        @Override
        public String idFromValue(Object value) {
            return value.getClass().getSimpleName();
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType) {
            return suggestedType.getSimpleName();
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            return JsonTypeInfo.Id.CUSTOM;
        }
    }

    public static class PlainBean {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "_class")
    @JsonTypeIdResolver(SimpleNameTypeIdResolver.class)
    public static abstract class AbstractBean {
    }

    public static class SubBean extends AbstractBean {
    }

    public static class SubSubBean extends SubBean {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "_class")
    @JsonTypeIdResolver(SimpleNameTypeIdResolver.class)
    public interface AnnotatedInterface {
    }

    public interface SubInterface extends AnnotatedInterface {
    }

    public static class InterfaceBean implements AnnotatedInterface {
    }

    public static class SubInterfaceBean extends InterfaceBean {
    }

    public static class SubInterfaceImplBean implements SubInterface {
    }

    public static class Holder {

        private Object nested;

        public Object getNested() {
            return nested;
        }

        public void setNested(Object nested) {
            this.nested = nested;
        }
    }
}

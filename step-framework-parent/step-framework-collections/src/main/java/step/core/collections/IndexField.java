package step.core.collections;

public class IndexField {

    public final String fieldName;
    public final Order order;
    public final Class<?> fieldClass;

    public IndexField(String fieldName, Order order, Class<?> fieldClass) {
        this.fieldName = fieldName;
        this.order = order;
        this.fieldClass = fieldClass;
    }
}

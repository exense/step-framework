package step.core.collections;

public class IndexField {

    private String fieldName;
    private int order;
    private Class<?> fieldClass;

    public IndexField(String fieldName, int order, Class<?> fieldClass) {
        this.fieldName = fieldName;
        this.order = order;
        this.fieldClass = fieldClass;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public Class<?> getFieldClass() {
        return fieldClass;
    }

    public void setFieldClass(Class<?> fieldClass) {
        this.fieldClass = fieldClass;
    }
}

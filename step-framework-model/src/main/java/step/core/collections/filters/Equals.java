package step.core.collections.filters;

import java.util.Objects;

public class Equals extends AbstractAtomicFilter {

    private String field;
    private Object expectedValue;

    public Equals() {
        super();
    }

    public Equals(String field, Object expectedValue) {
        super();
        this.field = field;
        this.expectedValue = expectedValue;
    }

    @Override
    public String getField() {
        return field;
    }

    public Object getExpectedValue() {
        return expectedValue;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setExpectedValue(Object expectedValue) {
        this.expectedValue = expectedValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Equals equals = (Equals) o;
        return Objects.equals(field, equals.field) && Objects.equals(expectedValue, equals.expectedValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, expectedValue);
    }
}

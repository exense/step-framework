package step.core.collections.filters;

import java.util.List;
import java.util.Objects;

public class In extends AbstractAtomicFilter {

    private String field;
    private List<Object> values;

    public In() {
        super();
    }

    public In(String field, List<Object> values) {
        super();
        Objects.requireNonNull(field, "The field of the In filter cannot be null.");
        Objects.requireNonNull(values, "The list of values of the In filter cannot be null.");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("The list of values of the In filter cannot be empty.");
        }
        this.field = field;
        this.values = values;
    }

    @Override
    public String getField() {
        return field;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        In in = (In) o;
        return values == in.values && Objects.equals(field, in.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, values);
    }
}

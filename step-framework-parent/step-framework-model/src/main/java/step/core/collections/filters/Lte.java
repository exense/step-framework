package step.core.collections.filters;

import java.util.Objects;

public class Lte extends AbstractAtomicFilter {

    private String field;
    private long value;

    public Lte() {
        super();
    }

    public Lte(String field, long value) {
        super();
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public long getValue() {
        return value;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lte lte = (Lte) o;
        return value == lte.value && Objects.equals(field, lte.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, value);
    }
}

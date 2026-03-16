package step.core.collections.filters;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

public abstract class AbstractLongAtomicFilter extends AbstractAtomicFilter {

    protected String field;
    protected long value;

    public AbstractLongAtomicFilter() {
        super();
    }
    public AbstractLongAtomicFilter(String field, long value) {
        super();
        this.field = field;
        this.value = value;
    }

    /**
     * Can be used by filter factories when type is relevant
     * Keep inline with the value field. This is a simple implementation to avoid reflection
     * @return the class of the value field
     */
    @JsonIgnore
    public Class<?> getValueType() {
        return long.class;
    }

    @Override
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
        AbstractLongAtomicFilter gt = (AbstractLongAtomicFilter) o;
        return value == gt.value && Objects.equals(field, gt.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, value);
    }
}

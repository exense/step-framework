package step.core.collections.filters;

public class Lte extends AbstractLongAtomicFilter {

    public Lte() {
        super();
    }

    public Lte(String field, long value) {
        super(field, value);
    }
}

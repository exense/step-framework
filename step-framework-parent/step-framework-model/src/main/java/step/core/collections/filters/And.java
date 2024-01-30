package step.core.collections.filters;

import step.core.collections.Filter;

import java.util.List;

public class And extends AbstractCompositeFilter {

    public And() {
        super();
    }

    public And(List<Filter> filters) {
        super(filters);
    }

}

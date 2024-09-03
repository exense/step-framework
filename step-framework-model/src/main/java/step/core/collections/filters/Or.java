package step.core.collections.filters;

import step.core.collections.Filter;

import java.util.List;

public class Or extends AbstractCompositeFilter {

    public Or() {
        super();
    }

    public Or(List<Filter> filters) {
        super(filters);
    }
}

package step.core.collections.filters;

import step.core.collections.Filter;

import java.util.ArrayList;
import java.util.List;

public class Not extends AbstractCompositeFilter {

    public Not() {
        super();
    }

    public Not(Filter filter) {
        super(new ArrayList<>(List.of(filter)));
    }
}

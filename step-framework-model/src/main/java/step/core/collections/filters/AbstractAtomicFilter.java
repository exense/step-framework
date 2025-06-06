package step.core.collections.filters;

import com.fasterxml.jackson.annotation.JsonIgnore;
import step.core.collections.Filter;

import java.util.List;

public abstract class AbstractAtomicFilter implements Filter {

    public AbstractAtomicFilter() {
        super();
    }

    @Override
    public List<Filter> getChildren() {
        return null;
    }

    public abstract String getField();

}

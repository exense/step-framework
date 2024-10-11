package step.core.collections.filters;

import step.core.collections.Filter;

import java.util.List;
import java.util.Objects;

public class AbstractCompositeFilter implements Filter {

    private List<Filter> children;

    public AbstractCompositeFilter() {
        super();
    }

    public AbstractCompositeFilter(List<Filter> children) {
        super();
        this.children = children;
    }

    @Override
    public String getField() {
        return null;
    }

    @Override
    public List<Filter> getChildren() {
        return children;
    }

    public void setChildren(List<Filter> children) {
        this.children = children;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractCompositeFilter that = (AbstractCompositeFilter) o;
        return Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(children);
    }
}

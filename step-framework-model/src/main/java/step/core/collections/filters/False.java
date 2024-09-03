package step.core.collections.filters;

import java.util.Objects;

public class False extends AbstractAtomicFilter {

    public False() {
        super();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass().hashCode());
    }
}

package step.core.collections.filters;

import java.util.Objects;

public class Fulltext extends AbstractAtomicFilter {

    private String expression;

    public Fulltext() {
        super();
    }

    public Fulltext(String expression) {
        super();
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fulltext fulltext = (Fulltext) o;
        return Objects.equals(expression, fulltext.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression);
    }
}

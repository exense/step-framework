package step.core.collections.filters;

import java.util.Objects;

public class Regex extends AbstractAtomicFilter {

    private String field;
    private String expression;
    private boolean caseSensitive;

    public Regex() {
        super();
    }

    public Regex(String field, String expression, boolean caseSensitive) {
        super();
        this.field = field;
        this.expression = expression;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public String getField() {
        return field;
    }

    public String getExpression() {
        return expression;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Regex regex = (Regex) o;
        return caseSensitive == regex.caseSensitive && Objects.equals(field, regex.field) && Objects.equals(expression, regex.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, expression, caseSensitive);
    }
}

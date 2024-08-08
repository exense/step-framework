package step.framework.server.tables.service;

import step.core.collections.Filter;
import step.core.collections.Filters;

public class FieldFilter extends TableFilter {

    private String field;
    private String value;
    private boolean isRegex;

    public FieldFilter() {
    }

    public FieldFilter(String field, String value, boolean isRegex) {
        this.field = field;
        this.value = value;
        this.isRegex = isRegex;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isRegex() {
        return isRegex;
    }

    public void setRegex(boolean regex) {
        isRegex = regex;
    }

    @Override
    public Filter toFilter() {
        if (isRegex) {
            return Filters.regex(field, value, false);
        } else {
            return Filters.equals(field, value);
        }
    }
}

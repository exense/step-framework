package step.framework.server.tables.service;

import step.core.collections.Filter;
import step.core.collections.Filters;

public class FulltextFilter extends TableFilter {

    private String text;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public Filter toFilter() {
        return Filters.fulltext(text);
    }
}

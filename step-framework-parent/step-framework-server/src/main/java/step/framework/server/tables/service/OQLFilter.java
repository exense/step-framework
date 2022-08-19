package step.framework.server.tables.service;

import step.core.collections.Filter;
import step.core.ql.OQLFilterBuilder;

public class OQLFilter extends TableFilter {

    private String oql;

    public String getOql() {
        return oql;
    }

    public void setOql(String oql) {
        this.oql = oql;
    }

    @Override
    public Filter toFilter() {
        return OQLFilterBuilder.getFilter(oql);
    }
}

package step.framework.server.tables.service;

import java.util.List;

public class TableQueryRequest {
    private List<TableFilter> filters;
    private TableParameters tableParameters;
    public List<TableFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<TableFilter> filters) {
        this.filters = filters;
    }

    public TableParameters getTableParameters() {
        return tableParameters;
    }

    public void setTableParameters(TableParameters tableParameters) {
        this.tableParameters = tableParameters;
    }

}

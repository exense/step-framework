package step.framework.server.tables.service;

import java.util.List;

public class TableRequest {

    private List<TableFilter> filters;

    private Integer skip;
    private Integer limit;

    private Sort sort;

    private TableParameters tableParameters;

    public List<TableFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<TableFilter> filters) {
        this.filters = filters;
    }

    public Integer getSkip() {
        return skip;
    }

    public void setSkip(Integer skip) {
        this.skip = skip;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public TableParameters getTableParameters() {
        return tableParameters;
    }

    public void setTableParameters(TableParameters tableParameters) {
        this.tableParameters = tableParameters;
    }
}

package step.framework.server.tables.service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class TableQueryRequest {
    private List<TableFilter> filters;
    private TableParameters tableParameters;
    private boolean internalRequest = false;

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


    // an internal request is, well, internal, so should neither be exposed to nor defined from JSON.
    @JsonIgnore
    public boolean isInternalRequest() {
        return internalRequest;
    }

    @JsonIgnore
    public void setInternalRequest(boolean internalRequest) {
        this.internalRequest = internalRequest;
    }

}

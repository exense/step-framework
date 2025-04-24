package step.framework.server.tables.service;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

public class TableRequest extends TableQueryRequest {

    private Integer skip;
    private Integer limit;

    private List<Sort> sort;
    private boolean performEnrichment = true;
    private boolean calculateCounts = true;

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

    public List<Sort> getSort() {
        return sort;
    }

    @JsonDeserialize(using = TableRequestSortDeserializer.class)
    public void setSort(List<Sort> sort) {
        this.sort = sort;
    }

    public boolean isPerformEnrichment() {
        return performEnrichment;
    }

    public void setPerformEnrichment(boolean performEnrichment) {
        this.performEnrichment = performEnrichment;
    }

    public boolean isCalculateCounts() {
        return calculateCounts;
    }

    public void setCalculateCounts(boolean calculateCounts) {
        this.calculateCounts = calculateCounts;
    }
}

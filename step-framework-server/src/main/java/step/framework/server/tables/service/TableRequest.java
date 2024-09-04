package step.framework.server.tables.service;

public class TableRequest extends TableQueryRequest {

    private Integer skip;
    private Integer limit;

    private Sort sort;
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

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
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

package step.framework.server.tables.service.bulk;

public class TableBulkOperationReport {

    private long count;

    public TableBulkOperationReport() {
    }

    public TableBulkOperationReport(long count) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}

package step.framework.server.tables.service.bulk;

import java.util.ArrayList;
import java.util.List;

public class TableBulkOperationReport {

    private long count;
    private long skipped;
    private long failed;
    private List<String> warnings;
    private List<String> errors;

    public TableBulkOperationReport(long count, long skipped, long failed, List<String> warnings, List<String> errors) {
        this.count = count;
        this.skipped = skipped;
        this.failed = failed;
        this.warnings = warnings;
        this.errors = errors;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSkipped() {
        return skipped;
    }

    public void setSkipped(long skipped) {
        this.skipped = skipped;
    }

    public long getFailed() {
        return failed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<String> warnings) {
        this.warnings = warnings;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
}

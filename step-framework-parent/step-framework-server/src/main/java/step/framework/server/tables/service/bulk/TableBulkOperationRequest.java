package step.framework.server.tables.service.bulk;

import step.framework.server.tables.service.TableQueryRequest;

import java.util.List;

public class TableBulkOperationRequest extends TableQueryRequest {

    private boolean preview = true;
    private TableBulkOperationTargetType targetType;
    private List<String> ids;

    public TableBulkOperationRequest() {
    }

    public TableBulkOperationRequest(boolean preview, TableBulkOperationTargetType targetType) {
        this.preview = preview;
        this.targetType = targetType;
    }

    public boolean isPreview() {
        return preview;
    }

    public void setPreview(boolean preview) {
        this.preview = preview;
    }

    public TableBulkOperationTargetType getTargetType() {
        return targetType;
    }

    public void setTargetType(TableBulkOperationTargetType targetType) {
        this.targetType = targetType;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }
}

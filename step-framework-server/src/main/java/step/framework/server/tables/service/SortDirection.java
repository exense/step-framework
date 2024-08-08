package step.framework.server.tables.service;

public enum SortDirection {

    ASCENDING(1),
    DESCENDING(-1);

    private final int value;

    SortDirection(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

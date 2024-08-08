package step.core.collections;

public enum Order {

    ASC(1),
    DESC(-1);

    public final int numeric;

    Order(int i) {
        this.numeric = i;
    }
}

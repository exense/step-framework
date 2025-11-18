package step.core.objectenricher;

// Define TriFunction since it's not available in Java 11
@FunctionalInterface
public interface TriFunction<T, U, V, R> {
    R apply(T t, U u, V v);
}

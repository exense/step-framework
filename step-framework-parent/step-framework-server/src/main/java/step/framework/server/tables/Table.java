package step.framework.server.tables;

import step.core.collections.Collection;
import step.core.collections.Filter;
import step.framework.server.Session;
import step.framework.server.tables.service.TableParameters;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class Table<T> {

    private final Collection<T> collection;
    private final String requiredAccessRight;

    private final boolean filtered;
    private BiFunction<TableParameters, Session<?>, Filter> tableFiltersFactory;

    private Integer maxFindDuration;
    private Integer countLimit;

    private Supplier<List<T>> resultListFactory;
    private Function<T, T> resultItemEnricher;

    /**
     * @param collection the collection backing this table
     * @param requiredAccessRight the right required to perform read requests on this table
     * @param filtered if the table is subject to context filtering (See {@link step.core.objectenricher.ObjectFilter})
     */
    public Table(Collection<T> collection, String requiredAccessRight, boolean filtered) {
        this.collection = collection;
        this.requiredAccessRight = requiredAccessRight;
        this.filtered = filtered;
    }

    /**
     * @param tableFiltersFactory a factory for additional filters to be applied at each table request
     * @return this instance
     */
    public Table<T> withTableFiltersFactory(BiFunction<TableParameters, Session<?>, Filter> tableFiltersFactory) {
        this.tableFiltersFactory = tableFiltersFactory;
        return this;
    }

    /**
     * @param tableFiltersFactory a factory for additional filters to be applied at each table request
     * @return this instance
     */
    public Table<T> withTableFiltersFactory(Function<TableParameters, Filter> tableFiltersFactory) {
        this.tableFiltersFactory = (parameters, session) -> tableFiltersFactory.apply(parameters);
        return this;
    }

    /**
     * @param maxFindDuration the maximal duration of the table requests in ms
     * @return this instance
     */
    public Table<T> withMaxFindDuration(int maxFindDuration) {
        this.maxFindDuration = maxFindDuration;
        return this;
    }

    /**
     * @param countLimit the limit to be used when counting the exact number of results for a table request
     * @return this instance
     */
    public Table<T> withCountLimit(int countLimit) {
        this.countLimit = countLimit;
        return this;
    }

    /**
     * Specififes a custom factory for the instantiation of the result list.
     * This can be used to create anonymous classes including the generic type
     * of the list which is sometimes required by Jackson
     * @param resultListFactory the factory to be used to create the result list
     * @return this instance
     */
    public Table<T> withResultListFactory(Supplier<List<T>> resultListFactory) {
        this.resultListFactory = resultListFactory;
        return this;
    }

    /**
     * @param resultItemEnricher an optional enricher to be use to enrich each element returned by this table.
     *                           The enricher is called for each element before returning the result
     * @return this instance
     */
    public Table<T> withResultItemEnricher(Function<T, T> resultItemEnricher) {
        this.resultItemEnricher = resultItemEnricher;
        return this;
    }

    public Collection<T> getCollection() {
        return collection;
    }

    public String getRequiredAccessRight() {
        return requiredAccessRight;
    }

    public boolean isFiltered() {
        return filtered;
    }

    public Optional<BiFunction<TableParameters, Session<?>, Filter>> getTableFiltersFactory() {
        return Optional.ofNullable(tableFiltersFactory);
    }

    public Optional<Integer> getMaxFindDuration() {
        return Optional.ofNullable(maxFindDuration);
    }

    public Optional<Integer> getCountLimit() {
        return Optional.ofNullable(countLimit);
    }

    public Optional<Supplier<List<T>>> getResultListFactory() {
        return Optional.ofNullable(resultListFactory);
    }

    public Optional<Function<T, T>> getResultItemEnricher() {
        return Optional.ofNullable(resultItemEnricher);
    }
}

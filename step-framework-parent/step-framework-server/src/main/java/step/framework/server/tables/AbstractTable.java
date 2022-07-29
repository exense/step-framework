package step.framework.server.tables;

import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.SearchOrder;
import step.framework.server.tables.service.TableParameters;

import java.util.Iterator;
import java.util.List;

public class AbstractTable<T> implements Table<T> {

    private final boolean filtered;
    protected final Collection<T> collection;
    private final int countLimit;
    private final int maxFindDuration;
    private final String requiredAccessRight;

    public AbstractTable(Collection<T> collection, String requiredAccessRight, boolean filtered) {
        this(collection, requiredAccessRight, filtered, 1000, 0);
    }

    public AbstractTable(Collection<T> collection, String requiredAccessRight, boolean filtered, int countLimit, int maxFindDuration) {
        this.collection = collection;
        this.requiredAccessRight = requiredAccessRight;
        this.filtered = filtered;
        this.countLimit = countLimit;
        this.maxFindDuration = maxFindDuration;
    }

    @Override
    public TableFindResult<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit) {
        Iterator<T> iterator = collection.find(filter, order, skip, limit, maxFindDuration).map(this::enrichEntity).iterator();
        long estimatedTotalCount = collection.estimatedCount();
        long count = collection.count(filter, countLimit);
        return new TableFindResult<>(estimatedTotalCount, count, iterator);
    }

    @Override
    public List<Filter> getTableFilters(TableParameters tableParameters) {
        return null;
    }

    @Override
    public boolean isContextFiltered() {
        return filtered;
    }

    @Override
    public String getRequiredAccessRight() {
        return requiredAccessRight;
    }

    public T enrichEntity(T entity) {
        return entity;
    }

}

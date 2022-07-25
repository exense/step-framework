package step.framework.server.tables;

import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.framework.server.tables.service.TableParameters;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AbstractTable<T> implements Table<T> {

    private final boolean filtered;
    protected final Collection<T> collection;
	private final int countLimit;
    private final int maxFindDuration;

	public AbstractTable(Collection<T> CollectionDriver, boolean filtered) {
        super();
        this.filtered = filtered;
        this.collection = CollectionDriver;
		this.countLimit = 1000;
        this.maxFindDuration = 0;
    }

	public AbstractTable(boolean filtered, Collection<T> collection, int countLimit, int maxFindDuration) {
		this.filtered = filtered;
		this.collection = collection;
		this.countLimit = countLimit;
        this.maxFindDuration = maxFindDuration;
	}

	@Override
    public List<String> distinct(String columnName, Filter filter) {
        return collection.distinct(columnName, filter).stream().filter(Objects::nonNull)
				.collect(Collectors.toList());
    }

    @Override
    public List<String> distinct(String columnName) {
        return distinct(columnName, Filters.empty());
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

    public T enrichEntity(T entity) {
        return entity;
    }

}

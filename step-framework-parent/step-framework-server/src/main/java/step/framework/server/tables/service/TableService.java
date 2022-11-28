package step.framework.server.tables.service;

import step.core.AbstractContext;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.core.objectenricher.ObjectFilter;
import step.core.objectenricher.ObjectHookRegistry;
import step.core.ql.OQLFilterBuilder;
import step.framework.server.Session;
import step.framework.server.access.AuthorizationManager;
import step.framework.server.tables.Table;
import step.framework.server.tables.TableRegistry;

import java.util.ArrayList;
import java.util.List;

public class TableService {

    private final TableRegistry tableRegistry;
    private final ObjectHookRegistry objectHookRegistry;
    private final AuthorizationManager authorizationManager;

    private final int defaultMaxFindDuration;
    private final int defaultMaxResultCount;

    public TableService(TableRegistry tableRegistry, ObjectHookRegistry objectHookRegistry, AuthorizationManager authorizationManager) {
        this(tableRegistry, objectHookRegistry, authorizationManager, 10, 1000);
    }

    public TableService(TableRegistry tableRegistry, ObjectHookRegistry objectHookRegistry, AuthorizationManager authorizationManager, int defaultMaxFindDuration, int defaultMaxResultCount) {
        this.tableRegistry = tableRegistry;
        this.objectHookRegistry = objectHookRegistry;
        this.authorizationManager = authorizationManager;
        this.defaultMaxFindDuration = defaultMaxFindDuration;
        this.defaultMaxResultCount = defaultMaxResultCount;
    }

    public <T> TableResponse<T> request(String tableName, TableRequest request, Session<?> session) throws TableServiceException {
        Table<T> table = (Table<T>) tableRegistry.get(tableName);
        if (table == null) {
            throw new TableServiceException("The table " + tableName + " doesn't exist");
        }
        return request(table, request, session);
    }

    public <T> TableResponse<T> request(Table<T> table, TableRequest request, Session<?> session) throws TableServiceException {
        String requiredAccessRight = table.getRequiredAccessRight();
        boolean hasRequiredRight = requiredAccessRight == null || authorizationManager.checkRightInContext(session, requiredAccessRight);
        if (hasRequiredRight) {
            // Create the filter
            final Filter filter = createFilter(request, session, table);

            // Get the search order
            SearchOrder searchOrder = getSearchOrder(request);

            // Create result list
            List<T> result = table.getResultListFactory().orElse(ArrayList::new).get();

            // Perform the search
            Collection<T> collection = table.getCollection();
            collection.find(filter, searchOrder, request.getSkip(), request.getLimit(), table.getMaxFindDuration().orElse(defaultMaxFindDuration))
                    .map(table.getResultItemEnricher().orElse(a -> a)).forEachOrdered(result::add);

            long estimatedTotalCount = collection.estimatedCount();
            long count = collection.count(filter, table.getCountLimit().orElse(defaultMaxResultCount));

            // Create the response
            TableResponse<T> response = new TableResponse<>();
            response.setRecordsFiltered(count);
            response.setRecordsTotal(estimatedTotalCount);
            response.setData(result);
            return response;
        } else {
            throw new TableServiceException("Missing right " + requiredAccessRight + " to access table");
        }
    }

    private SearchOrder getSearchOrder(TableRequest request) {
        SearchOrder searchOrder;
        Sort sort = request.getSort();
        if (sort != null) {
            searchOrder = new SearchOrder(sort.getField(), sort.getDirection().getValue());
        } else {
            searchOrder = null;
        }
        return searchOrder;
    }

    private Filter createFilter(TableRequest request, AbstractContext context, Table<?> table) {
        ArrayList<Filter> filters = new ArrayList<>();

        // Add object filter from context
        if (table.isFiltered()) {
            ObjectFilter objectFilter = objectHookRegistry.getObjectFilter(context);
            filters.add(OQLFilterBuilder.getFilter(objectFilter.getOQLFilter()));
        }

        // Add table specific filters
        table.getTableFiltersFactory().ifPresent(factory -> filters.add(factory.apply(request.getTableParameters())));

        // Add requested filters
        List<TableFilter> requestFilters = request.getFilters();
        if (requestFilters != null) {
            requestFilters.forEach(f -> filters.add(f.toFilter()));
        }

        // Create final filter
        return filters.size() > 0 ? Filters.and(filters) : Filters.empty();
    }
}

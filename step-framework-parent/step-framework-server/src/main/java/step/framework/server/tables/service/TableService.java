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
import step.framework.server.access.AccessManager;
import step.framework.server.tables.Table;
import step.framework.server.tables.TableRegistry;

import java.util.ArrayList;
import java.util.List;

public class TableService {

    private final TableRegistry tableRegistry;
    private final ObjectHookRegistry objectHookRegistry;
    private final AccessManager accessManager;

    private final int defaultMaxFindDuration;
    private final int defaultMaxResultCount;

    public TableService(TableRegistry tableRegistry, ObjectHookRegistry objectHookRegistry, AccessManager accessManager) {
        this(tableRegistry, objectHookRegistry, accessManager, 10000, 1000);
    }

    public TableService(TableRegistry tableRegistry, ObjectHookRegistry objectHookRegistry, AccessManager accessManager, int defaultMaxFindDuration, int defaultMaxResultCount) {
        this.tableRegistry = tableRegistry;
        this.objectHookRegistry = objectHookRegistry;
        this.accessManager = accessManager;
        this.defaultMaxFindDuration = defaultMaxFindDuration;
        this.defaultMaxResultCount = defaultMaxResultCount;
    }

    public TableResponse request(String tableName, TableRequest request, Session<?> session) throws TableServiceException {
        Table<Object> table = (Table<Object>) tableRegistry.get(tableName);
        if (table == null) {
            throw new TableServiceException("The table " + tableName + " doesn't exist");
        }

        String requiredAccessRight = table.getRequiredAccessRight();
        boolean hasRequiredRight = requiredAccessRight == null || accessManager.checkRightInContext(session, requiredAccessRight);
        if (hasRequiredRight) {
            // Create the filter
            final Filter filter = createFilter(request, session, table);

            // Get the search order
            SearchOrder searchOrder = getSearchOrder(request);

            // Create result list
            List<Object> result = table.getResultListFactory().orElse(ArrayList::new).get();

            // Perform the search
            Collection<?> collection = table.getCollection();
            collection.find(filter, searchOrder, request.getSkip(), request.getLimit(), table.getMaxFindDuration().orElse(defaultMaxFindDuration))
                    .map(table.getResultItemEnricher().orElse(a -> a)).forEachOrdered(result::add);

            long estimatedTotalCount = collection.estimatedCount();
            long count = collection.count(filter, table.getCountLimit().orElse(defaultMaxResultCount));

            // Create the response
            TableResponse response = new TableResponse();
            response.setRecordsFiltered(count);
            response.setRecordsTotal(estimatedTotalCount);
            response.setData(result);
            return response;
        } else {
            throw new TableServiceException("Missing right " + requiredAccessRight + " to access table " + tableName);
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

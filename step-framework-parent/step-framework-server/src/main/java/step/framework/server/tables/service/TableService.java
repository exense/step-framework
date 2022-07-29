package step.framework.server.tables.service;

import org.apache.commons.collections.IteratorUtils;
import step.core.AbstractContext;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.core.objectenricher.ObjectFilter;
import step.core.objectenricher.ObjectHookRegistry;
import step.core.ql.OQLFilterBuilder;
import step.framework.server.Session;
import step.framework.server.access.AccessManager;
import step.framework.server.tables.Table;
import step.framework.server.tables.TableFindResult;
import step.framework.server.tables.TableRegistry;

import java.util.ArrayList;
import java.util.List;

public class TableService {

    private final AccessManager accessManager;
    private final TableRegistry tableRegistry;
    private final ObjectHookRegistry objectHookRegistry;

    public TableService(TableRegistry tableRegistry, ObjectHookRegistry objectHookRegistry, AccessManager accessManager) {
        this.tableRegistry = tableRegistry;
        this.objectHookRegistry = objectHookRegistry;
        this.accessManager = accessManager;
    }

    public TableResponse<?> request(String tableName, TableRequest request, Session session) throws TableServiceException {
        Table<?> table = tableRegistry.get(tableName);
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

            // Perform the search
            TableFindResult<?> result = table.find(filter, searchOrder, request.getSkip(), request.getLimit());

            TableResponse<Object> response = new TableResponse<>();
            response.setRecordsFiltered(result.getRecordsFiltered());
            response.setRecordsTotal(result.getRecordsTotal());
            response.setData(IteratorUtils.toList(result.getIterator()));
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
        if (table.isContextFiltered()) {
            ObjectFilter objectFilter = objectHookRegistry.getObjectFilter(context);
            filters.add(OQLFilterBuilder.getFilter(objectFilter.getOQLFilter()));
        }

        // Add table specific filters
        List<Filter> additionalQueryFragments = table.getTableFilters(request.getTableParameters());
        if (additionalQueryFragments != null) {
            filters.addAll(additionalQueryFragments);
        }

        // Add requested filters
        List<TableFilter> requestFilters = request.getFilters();
        if (requestFilters != null) {
            requestFilters.forEach(f -> filters.add(f.toFilter()));
        }

        // Create final filter
        return filters.size() > 0 ? Filters.and(filters) : Filters.empty();
    }
}

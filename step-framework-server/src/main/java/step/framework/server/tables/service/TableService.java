package step.framework.server.tables.service;

import step.core.accessors.AbstractIdentifiableObject;
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
import step.framework.server.tables.service.bulk.BulkOperationWarningException;
import step.framework.server.tables.service.bulk.TableBulkOperationReport;
import step.framework.server.tables.service.bulk.TableBulkOperationRequest;
import step.framework.server.tables.service.bulk.TableBulkOperationTargetType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Table<T> table = getTable(tableName);
        return request(table, request, session);
    }

    public <T> TableResponse<T> request(Table<T> table, TableRequest request, Session<?> session) throws TableServiceException {
        // Assert right
        assertSessionHasRequiredAccessRight(table, session);

        // Create the filter
        final Filter filter = createFilter(request, session, table);

        // Create result list
        List<T> result = table.getResultListFactory().orElse(ArrayList::new).get();

        // Perform the search
        Collection<T> collection = table.getCollection();
        try (Stream<T> tStream = _request(collection, table, filter, request, session)) {
            tStream.forEachOrdered(result::add);
        }

        // Calculate counts
        long estimatedTotalCount;
        long count;
        if (request.isCalculateCounts()) {
            estimatedTotalCount = collection.estimatedCount();
            count = collection.count(filter, table.getCountLimit().orElse(defaultMaxResultCount));
        } else {
            estimatedTotalCount = -1;
            count = -1;
        }

        // Create the response
        TableResponse<T> response = new TableResponse<>();
        response.setRecordsFiltered(count);
        response.setRecordsTotal(estimatedTotalCount);
        response.setData(result);
        return response;
    }

    private <T> Stream<T> _request(Collection<T> collection, Table<T> table, Filter filter, TableRequest request, Session<?> session) {
        // Get the search order
        SearchOrder searchOrder = getSearchOrder(request, table);

        // Perform the search
        Stream<T> result = collection.findLazy(filter, searchOrder, request.getSkip(), request.getLimit(), table.getMaxFindDuration().orElse(defaultMaxFindDuration));
        Optional<BiFunction<T, Session<?>, T>> transformer = table.getResultItemTransformer();
        if (transformer.isPresent()) {
            result = result.map(item -> transformer.get().apply(item, session));
        }

        if (request.isPerformEnrichment()) {
            Optional<BiFunction<T, Session<?>, T>> enricher = table.getResultItemEnricher();
            if (enricher.isPresent()) {
                result = result.map(item -> enricher.get().apply(item, session));
            }
        }
        return result;
    }

    public <T> Stream<T> export(String tableName, TableRequest request, Session<?> session) throws TableServiceException {
        Table<T> table = getTable(tableName);
        // Assert right
        assertSessionHasRequiredAccessRight(table, session);

        Collection<T> collection = table.getCollection();
        // Create the filter
        final Filter filter = createFilter(request, session, table);
        return _request(collection, table, filter, request, session);
    }

    public <T extends AbstractIdentifiableObject> TableBulkOperationReport performBulkOperationWithCustomPreview(
            String tableName, TableBulkOperationRequest parameters, BiConsumer<String, Boolean> operationById,
            Session<?> session) throws TableServiceException {
        Table<T> table = getTable(tableName);
        return performBulkOperationWithCustomPreview(table, parameters, operationById, session);
    }

    public <T extends AbstractIdentifiableObject> TableBulkOperationReport performBulkOperationWithCustomPreview(
            Table<T> table, TableBulkOperationRequest parameters, BiConsumer<String, Boolean> operationById,
            Session<?> session) throws TableServiceException {
        // assert rights
        assertSessionHasRequiredAccessRight(table, session);
        // validate parameters
        TableBulkOperationTargetType targetType = parameters.getTargetType();
        validateParameters(parameters, targetType);

        LongAdder okCount = new LongAdder();
        Map<String, LongAdder> warnings = new ConcurrentHashMap<>();
        Map<String, LongAdder> errors = new ConcurrentHashMap<>();
        BiConsumer<String, Boolean> countingOperationById = (id, preview) -> {
            try {
                operationById.accept(id, parameters.isPreview());
                okCount.increment();
            } catch (BulkOperationWarningException e) {
                LongAdder warningCount = warnings.computeIfAbsent(e.getMessage(), a -> new LongAdder());
                warningCount.increment();
            } catch (Exception e) {
                LongAdder errorCount = errors.computeIfAbsent(e.getMessage(), a -> new LongAdder());
                errorCount.increment();
            }
        };

        // Perform bulk operation
        if (targetType == TableBulkOperationTargetType.LIST) {
            List<String> ids = parameters.getIds();
            if (ids != null && !ids.isEmpty()) {
                ids.forEach(id -> countingOperationById.accept(id, parameters.isPreview()));
            }
        } else {
            if (targetType == TableBulkOperationTargetType.FILTER || targetType == TableBulkOperationTargetType.ALL) {
                Filter filter = createFilter(parameters, session, table);
                Collection<T> collection = table.getCollection();
                //Storing the ids in memory to avoid mongodb cursor timeout exception (housekeeping of executions)
                List<String> ids = collection.find(filter, null, null, null, 0).map(e -> e.getId().toString()).collect(Collectors.toList());
                ids.forEach(id -> countingOperationById.accept(id, parameters.isPreview()));
            } else {
                throw new TableServiceException("Unsupported targetFilter" + targetType);
            }
        }

        List<String> warningMessages = new ArrayList<>();
        LongAdder skipped = new LongAdder();
        warnings.forEach((k, v) -> {
            warningMessages.add(k + " (" + v.longValue() + " occurrences)");
            skipped.add(v.longValue());
        });
        List<String> errorMessages = new ArrayList<>();
        LongAdder errorCount = new LongAdder();
        errors.forEach((k, v) -> {
            errorMessages.add(k + " (" + v.longValue() + " occurrences)");
            errorCount.add(v.longValue());
        });
        TableBulkOperationReport tableBulkOperationReport = new TableBulkOperationReport(okCount.longValue(), skipped.longValue(),
                errorCount.longValue(), warningMessages, errorMessages);
        return tableBulkOperationReport;
    }

    public <T extends AbstractIdentifiableObject> TableBulkOperationReport performBulkOperation(
            String tableName, TableBulkOperationRequest request, Consumer<String> operationById,
            Session<?> session) throws TableServiceException {
        Table<T> table = getTable(tableName);
        return performBulkOperation(table, request, operationById, session);
    }

    public <T extends AbstractIdentifiableObject> TableBulkOperationReport performBulkOperation(
            Table<T> table, TableBulkOperationRequest request, Consumer<String> operationById,
            Session<?> session) throws TableServiceException {
        BiConsumer<String, Boolean> previewAwareOperationById = (id, preview) -> {
            if (!preview) {
                operationById.accept(id);
            }
        };
        return performBulkOperationWithCustomPreview(table, request, previewAwareOperationById, session);
    }

    private <T> Table<T> getTable(String tableName) throws TableServiceException {
        Table<T> table = (Table<T>) tableRegistry.get(tableName);
        if (table == null) {
            throw new TableServiceException("The table " + tableName + " doesn't exist");
        }
        return table;
    }

    private void validateParameters(TableBulkOperationRequest parameters, TableBulkOperationTargetType targetType) throws TableServiceException {
        if (targetType == TableBulkOperationTargetType.LIST) {
            List<String> ids = parameters.getIds();
            if (ids == null || ids.isEmpty()) {
                throw new TableServiceException("No Ids specified. Please specify a list of entity Ids to be processed");
            }
        } else if (targetType == TableBulkOperationTargetType.FILTER) {
            List<TableFilter> filters = parameters.getFilters();
            if (filters == null) {
                throw new TableServiceException("No filter specified. Please specify filter for the entities to to be processed");
            }
        } else if (targetType == TableBulkOperationTargetType.ALL) {
            if (parameters.getFilters() != null || parameters.getIds() != null) {
                throw new TableServiceException("No filter or Ids should be specified using target ALL.");
            }
        }
    }

    private <T> void assertSessionHasRequiredAccessRight(Table<T> table, Session<?> session) throws TableServiceException {
        String requiredAccessRight = table.getRequiredAccessRight();
        boolean hasRequiredRight = requiredAccessRight == null || authorizationManager.checkRightInContext(session, requiredAccessRight);
        if (!hasRequiredRight) {
            throw new TableServiceException("Missing right " + requiredAccessRight + " to access table");
        }
    }

    private <T>  SearchOrder getSearchOrder(TableRequest request, Table<T> table) {
        SearchOrder searchOrder = null;
        Sort sort = request.getSort();
        if (sort != null) {
            if (table.getDerivedTableSortingFactory() != null) {
                searchOrder = table.getDerivedTableSortingFactory().apply(sort);
            }
            if (searchOrder == null) {
                searchOrder = new SearchOrder(sort.getField(), sort.getDirection().getValue());
            }
        }
        return searchOrder;
    }

    protected Filter createFilter(TableQueryRequest request, Session<?> session, Table<?> table) {
        ArrayList<Filter> filters = new ArrayList<>();

        // Add object filter from context
        if (table.isFiltered()) {
            ObjectFilter objectFilter = objectHookRegistry.getObjectFilter(session);
            filters.add(OQLFilterBuilder.getFilter(objectFilter.getOQLFilter()));
        }

        // Add table specific filters
        table.getTableFiltersFactory().ifPresent(factory -> filters.add(factory.apply(request.getTableParameters(), session)));

        // Add requested filters
        List<TableFilter> requestFilters = request.getFilters();
        if (requestFilters != null) {
            requestFilters.forEach(f -> filters.add(f.toFilter()));
        }
        table.getDerivedTableFiltersFactory().ifPresent(factory -> filters.add(factory.apply(filters)));

        // Create final filter
        return filters.size() > 0 ? Filters.and(filters) : Filters.empty();
    }
}

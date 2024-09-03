package step.framework.server.tables.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import step.core.AbstractContext;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.filters.Not;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.entities.Bean;
import step.core.objectenricher.*;
import step.framework.server.Session;
import step.framework.server.access.NoAuthorizationManager;
import step.framework.server.tables.Table;
import step.framework.server.tables.TableRegistry;
import step.framework.server.tables.service.bulk.TableBulkOperationReport;
import step.framework.server.tables.service.bulk.TableBulkOperationRequest;
import step.framework.server.tables.service.bulk.TableBulkOperationTargetType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class TableServiceTest {

    private static final String TEST_RIGHT = "test-right";
    private static final String SIMPLE_TABLE = "table1";
    private static final String FILTERED_TABLE = "filteredTable";
    private static final String TABLE_WITH_ACCESS_RIGHT = "tableWithAccessRight";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";
    private static final String VALUE_3 = "value3";
    private static final String ENRICHED_ATTRIBUTE_KEY = "entichedAttributes";
    private static final String ENRICHED_ATTRIBUTE_VALUE = "value";
    private static final String TABLE_WITH_FILTER_FACTORY = "tableWithFilterFactory";
    private static final String TABLE_WITH_FILTER_FACTORY_WITH_SESSION = "tableWithFilterFactoryWithSession";
    private Bean bean1;
    private Bean bean2;
    private Bean bean3;
    private Collection<Bean> collection;

    private final List<String> processedIds = new ArrayList<>();
    private Table<Bean> table;

    @Before
    public void before() throws TableServiceException {
        collection = new InMemoryCollection<>();
        bean1 = new Bean(VALUE_1);
        bean2 = new Bean(VALUE_2);
        bean3 = new Bean(VALUE_3);

        collection.save(bean1);
        collection.save(bean2);
        collection.save(bean3);
        table = new Table<>(collection, TEST_RIGHT, true);
    }

    @Test
    public void request() throws TableServiceException {
        TableRegistry tableRegistry = new TableRegistry();
        Table<Bean> table = new Table<>(collection, null, false);
        tableRegistry.register(SIMPLE_TABLE, table);

        Table<Bean> filteredTable = new Table<>(collection, null, true);
        tableRegistry.register(FILTERED_TABLE, filteredTable);

        Table<Bean> tableWithFilterFactory = new Table<>(collection, null, false);
        tableWithFilterFactory.withTableFiltersFactory(p->Filters.equals("property1", VALUE_1));
        tableRegistry.register(TABLE_WITH_FILTER_FACTORY, tableWithFilterFactory);

        Table<Bean> tableWithFilterFactoryWithSession = new Table<>(collection, null, false);
        tableWithFilterFactoryWithSession.withTableFiltersFactory((p, session) -> Filters.and(List.of(Filters.equals("property1", VALUE_1),
                (session != null && session.get(TEST_RIGHT) != null) ? Filters.empty() : Filters.falseFilter())));
        tableRegistry.register(TABLE_WITH_FILTER_FACTORY_WITH_SESSION, tableWithFilterFactoryWithSession);

        Table<Bean> tableWithAccessRight = new Table<>(collection, TEST_RIGHT, false);
        tableRegistry.register(TABLE_WITH_ACCESS_RIGHT, tableWithAccessRight);

        TableService tableService = new TableService(tableRegistry, objectHookRegistryWithContextFilter(() -> "property1 = " + VALUE_2), new TestAuthorizationManager());

        // Test empty request
        TableRequest request = new TableRequest();
        TableResponse<?> response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean1, bean2, bean3), response.getData());
        assertEquals(3, response.getRecordsTotal());
        assertEquals(3, response.getRecordsFiltered());

        // Test request with count disabled
        request = new TableRequest();
        request.setCalculateCounts(false);
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean1, bean2, bean3), response.getData());
        assertEquals(-1, response.getRecordsTotal());
        assertEquals(-1, response.getRecordsFiltered());


        // Test FieldFilter
        request = new TableRequest();
        request.setFilters(List.of(new FieldFilter("property1", VALUE_1, false)));
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean1), response.getData());
        assertEquals(3, response.getRecordsTotal());
        assertEquals(1, response.getRecordsFiltered());

        // Test skip limit
        request = new TableRequest();
        request.setSkip(1);
        request.setLimit(1);
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean2), response.getData());

        // Test skip limit
        request = new TableRequest();
        Sort sort = new Sort();
        sort.setField("property1");
        sort.setDirection(SortDirection.DESCENDING);
        request.setSort(sort);
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean3, bean2, bean1), response.getData());

        // Test custom ResultItemEnricher
        table.withResultItemEnricher(e -> {
            e.addAttribute(ENRICHED_ATTRIBUTE_KEY, ENRICHED_ATTRIBUTE_VALUE);
            return e;
        });
        request = new TableRequest();
        request.setFilters(List.of(new FieldFilter("property1", VALUE_1, false)));
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean1), response.getData());
        Bean actualBean = (Bean) response.getData().get(0);
        assertEquals(ENRICHED_ATTRIBUTE_VALUE, actualBean.getAttribute(ENRICHED_ATTRIBUTE_KEY));

        // Test request with enrichment disabled
        bean1.getAttributes().clear();
        request = new TableRequest();
        request.setPerformEnrichment(false);
        request.setFilters(List.of(new FieldFilter("property1", VALUE_1, false)));
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean1), response.getData());
        actualBean = (Bean) response.getData().get(0);
        assertFalse(actualBean.hasAttribute(ENRICHED_ATTRIBUTE_KEY));

        // Test custom ResultListFactory
        ArrayList<Bean> customResultList = new ArrayList<>();
        table.withResultListFactory(() -> customResultList);
        request = new TableRequest();
        response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(customResultList, response.getData());

        // Test FilterFactory
        request = new TableRequest();
        response = tableService.request(TABLE_WITH_FILTER_FACTORY, request, null);
        assertEquals(List.of(bean1), response.getData());

        request = new TableRequest();
        response = tableService.request(TABLE_WITH_FILTER_FACTORY_WITH_SESSION, request, sessionWithRight());
        assertEquals(List.of(bean1), response.getData());

        request = new TableRequest();
        response = tableService.request(TABLE_WITH_FILTER_FACTORY_WITH_SESSION, request, null);
        assertEquals(List.of(), response.getData());

        request = new TableRequest();
        response = tableService.request(TABLE_WITH_FILTER_FACTORY_WITH_SESSION, request, new Session<>());
        assertEquals(List.of(), response.getData());

        // Test filtered table
        request = new TableRequest();
        response = tableService.request(FILTERED_TABLE, request, null);
        assertEquals(List.of(bean2), response.getData());

        // Query a table requiring a right with the correct right
        request = new TableRequest();
        Session<?> session = new Session<>();
        session.put(TEST_RIGHT, new Object());
        response = tableService.request(TABLE_WITH_ACCESS_RIGHT, request, session);
        assertNotNull(response);

        // Query a table requiring a right without the required right in session
        assertThrows(TableServiceException.class, () -> tableService.request(TABLE_WITH_ACCESS_RIGHT, new TableRequest(), new Session<>()));

        // Query a table requiring a right without session
        assertThrows(TableServiceException.class, () -> tableService.request(TABLE_WITH_ACCESS_RIGHT, new TableRequest(), null));

        // Test non existing table
        Assert.assertThrows(TableServiceException.class, () -> tableService.request("invalid", null, null));
    }

    @Test
    public void testSerialization() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectReader reader = objectMapper.reader();
        ObjectWriter writer = objectMapper.writer();
        TableRequest tableRequest = reader.readValue("{}", TableRequest.class);
        assertNull(tableRequest.getFilters());

        tableRequest = reader.readValue("{\"filters\":[{\"field\":\"myField\", \"value\":\"myValue\", \"regex\":true}]}", TableRequest.class);
        assertEquals(1, tableRequest.getFilters().size());
        FieldFilter filter = (FieldFilter) tableRequest.getFilters().get(0);
        assertEquals("myField", filter.getField());
        assertEquals("myValue", filter.getValue());
        assertTrue(filter.isRegex());

        writer.writeValueAsString(tableRequest);

        tableRequest = reader.readValue("{\"filters\":[{\"text\":\"my full text\"}]}", TableRequest.class);
        assertEquals(1, tableRequest.getFilters().size());
        FulltextFilter fulltextFilter = (FulltextFilter) tableRequest.getFilters().get(0);
        assertEquals("my full text", fulltextFilter.getText());

        writer.writeValueAsString(tableRequest);

        tableRequest = reader.readValue("{\"filters\":[{\"oql\":\"not(test=test)\"}]}", TableRequest.class);
        assertEquals(1, tableRequest.getFilters().size());
        OQLFilter oqlFilter = (OQLFilter) tableRequest.getFilters().get(0);
        assertEquals("not(test=test)", oqlFilter.getOql());
        Filter oqlFilter_ = oqlFilter.toFilter();
        assertEquals(Not.class, oqlFilter_.getClass());

        writer.writeValueAsString(tableRequest);

        tableRequest = reader.readValue("{\"filters\":[{\"collectionFilter\":{\"type\":\"True\"}}]}", TableRequest.class);
        assertEquals(1, tableRequest.getFilters().size());
        CollectionFilter collectionFilter = (CollectionFilter) tableRequest.getFilters().get(0);
        assertEquals(Filters.empty(), collectionFilter.getCollectionFilter());

        writer.writeValueAsString(tableRequest);

        tableRequest = reader.readValue("{\"sort\":{\"field\":\"field1\", \"direction\":\"ASCENDING\"}}", TableRequest.class);
        assertEquals("field1", tableRequest.getSort().getField());
        assertEquals(SortDirection.ASCENDING, tableRequest.getSort().getDirection());

        writer.writeValueAsString(tableRequest);

        tableRequest = reader.readValue("{\"tableParameters\": {\"type\":\"" + MyTableParameters.class.getName() + "\"}}", TableRequest.class);
        assertNotNull(tableRequest.getTableParameters());
    }

    @Test
    public void performBulkOperation() throws TableServiceException {
        TableBulkOperationRequest parameters = new TableBulkOperationRequest(false, TableBulkOperationTargetType.LIST);
        assertThrows(TableServiceException.class, () -> tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight()));

        // performBulkOperation by table
        List<String> targetIds = List.of(bean1.getId().toString(), bean2.getId().toString());
        parameters.setIds(targetIds);
        TableBulkOperationReport exportStatus = tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        assertEquals(targetIds, processedIds);
        assertEquals(2, exportStatus.getCount());

        // performBulkOperation by table name
        processedIds.clear();
        exportStatus = tableService().performBulkOperation(SIMPLE_TABLE, parameters, processedIds::add, sessionWithRight());
        assertEquals(targetIds, processedIds);
        assertEquals(2, exportStatus.getCount());

        // performBulkOperationWithCustomPreview by table name
        processedIds.clear();
        exportStatus = tableService().performBulkOperationWithCustomPreview(SIMPLE_TABLE, parameters, (id, preview) -> processedIds.add(id), sessionWithRight());
        assertEquals(targetIds, processedIds);
        assertEquals(2, exportStatus.getCount());
    }

    @Test
    public void performBulkOperationPreview() throws TableServiceException {
        TableBulkOperationRequest parameters = new TableBulkOperationRequest(true, TableBulkOperationTargetType.LIST);
        parameters.setIds(List.of(bean1.getId().toString(), bean2.getId().toString()));
        TableBulkOperationReport exportStatus = tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        // Ensure that no processing has been done in preview mode
        assertEquals(List.of(), processedIds);
        assertEquals(2, exportStatus.getCount());

        // performBulkOperation by table name
        processedIds.clear();
        exportStatus = tableService().performBulkOperation(SIMPLE_TABLE, parameters, processedIds::add, sessionWithRight());
        // Ensure that no processing has been done in preview mode
        assertEquals(List.of(), processedIds);
        assertEquals(2, exportStatus.getCount());

        // performBulkOperationWithCustomPreview by table name
        processedIds.clear();
        exportStatus = tableService().performBulkOperationWithCustomPreview(SIMPLE_TABLE, parameters, (id, preview) -> processedIds.add(id), sessionWithRight());
        assertEquals(List.of(bean1.getId().toString(), bean2.getId().toString()), processedIds);
        assertEquals(2, exportStatus.getCount());
    }

    @Test
    public void performBulkOperationWithCustomPreview() throws TableServiceException {
        TableBulkOperationRequest parameters = new TableBulkOperationRequest(true, TableBulkOperationTargetType.LIST);
        List<String> targetIds = List.of(bean1.getId().toString(), bean2.getId().toString());
        parameters.setIds(targetIds);
        AtomicBoolean actualPreview = new AtomicBoolean();
        TableBulkOperationReport exportStatus = tableService().performBulkOperationWithCustomPreview(table, parameters, (id, preview) -> {
            processedIds.add(id);
            actualPreview.set(preview);
        }, sessionWithRight());
        assertEquals(targetIds, processedIds);
        assertEquals(2, exportStatus.getCount());
        assertTrue(actualPreview.get());

        processedIds.clear();
        parameters.setPreview(false);
        exportStatus = tableService().performBulkOperationWithCustomPreview(table, parameters, (id, preview) -> {
            processedIds.add(id);
            actualPreview.set(preview);
        }, sessionWithRight());
        assertEquals(targetIds, processedIds);
        assertEquals(2, exportStatus.getCount());
        assertFalse(actualPreview.get());
    }

    @Test
    public void performBulkOperationFilter() throws TableServiceException {
        // Missing filters
        TableBulkOperationRequest parameters = new TableBulkOperationRequest(false, TableBulkOperationTargetType.FILTER);
        assertThrows(TableServiceException.class, () -> tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight()));

        // Test one filter
        parameters.setFilters(List.of(new FieldFilter("property1", "value1", true)));
        TableBulkOperationReport exportStatus = tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        assertEquals(List.of(bean1.getId().toString()), processedIds);
        assertEquals(1, exportStatus.getCount());

        // Test multiple filters
        processedIds.clear();
        parameters.setFilters(List.of(new FieldFilter("property1", "value1", true),
                new FieldFilter("property1", "value2", true)));
        exportStatus = tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        assertEquals(List.of(), processedIds);
        assertEquals(0, exportStatus.getCount());

        // Test context filter
        processedIds.clear();
        parameters.setFilters(List.of());
        exportStatus = tableService(() -> "property1 = 'value1'").performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        assertEquals(List.of(bean1.getId().toString()), processedIds);
        assertEquals(1, exportStatus.getCount());

        // Test one filter and context filter
        processedIds.clear();
        parameters.setFilters(List.of(new FieldFilter("property1", "value1", true)));
        exportStatus = tableService(() -> "property1 = 'value2'").performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        assertEquals(List.of(), processedIds);
        assertEquals(0, exportStatus.getCount());
    }

    @Test
    public void performBulkOperationAll() throws TableServiceException {
        // All with specified Ids
        TableBulkOperationRequest parameters = new TableBulkOperationRequest(false, TableBulkOperationTargetType.ALL);
        parameters.setIds(List.of());
        assertThrows(TableServiceException.class, () -> tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight()));

        // All with specified filters
        parameters.setIds(null);
        parameters.setFilters(List.of(new FieldFilter()));
        assertThrows(TableServiceException.class, () -> tableService().performBulkOperation(table, parameters, processedIds::add, sessionWithRight()));

        // All with valid parameters
        parameters.setIds(null);
        parameters.setFilters(null);
        TableBulkOperationReport exportStatus = tableService(() -> "property1 = 'value1'").performBulkOperation(table, parameters, processedIds::add, sessionWithRight());
        assertEquals(List.of(bean1.getId().toString()), processedIds);
        assertEquals(1, exportStatus.getCount());
    }

    private Session<?> sessionWithRight() {
        Session<?> session = new Session<>();
        session.put(TEST_RIGHT, new Object());
        return session;
    }

    private static class MyTableParameters extends TableParameters {

    }

    private static class TestAuthorizationManager extends NoAuthorizationManager {

        @Override
        public boolean checkRightInContext(Session session, String right) {
            return session != null && session.get(right) != null;
        }

    }

    private ObjectHookRegistry objectHookRegistryWithContextFilter(ObjectFilter contextObjectFilter) {
        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new ObjectHook() {
            @Override
            public ObjectFilter getObjectFilter(AbstractContext context) {
                return contextObjectFilter;
            }

            @Override
            public ObjectEnricher getObjectEnricher(AbstractContext context) {
                return null;
            }

            @Override
            public void rebuildContext(AbstractContext context, EnricheableObject object) {
            }

            @Override
            public boolean isObjectAcceptableInContext(AbstractContext context, EnricheableObject object) {
                return false;
            }
        });
        return objectHookRegistry;
    }

    private TableService tableService() {
        return tableService(() -> "");
    }
    private TableService tableService(ObjectFilter contextObjectFilter) {
        TableRegistry tableRegistry = new TableRegistry();
        tableRegistry.register(SIMPLE_TABLE, table);
        return new TableService(tableRegistry, objectHookRegistryWithContextFilter(contextObjectFilter), new TestAuthorizationManager());
    }

}

package step.framework.server.tables.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
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
import step.framework.server.access.AccessManager;
import step.framework.server.access.NoAccessManager;
import step.framework.server.tables.Table;
import step.framework.server.tables.TableRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void request() throws TableServiceException {
        Collection<Bean> collection = new InMemoryCollection<>();
        Bean bean1 = new Bean(VALUE_1);
        Bean bean2 = new Bean(VALUE_2);
        Bean bean3 = new Bean(VALUE_3);

        collection.save(bean1);
        collection.save(bean2);
        collection.save(bean3);

        TableRegistry tableRegistry = new TableRegistry();
        Table<Bean> table = new Table<>(collection, null, false);
        tableRegistry.register(SIMPLE_TABLE, table);

        Table<Bean> filteredTable = new Table<>(collection, null, true);
        tableRegistry.register(FILTERED_TABLE, filteredTable);

        Table<Bean> tableWithFilterFactory = new Table<>(collection, null, false);
        tableWithFilterFactory.withTableFiltersFactory(p->Filters.equals("property1", VALUE_1));
        tableRegistry.register(TABLE_WITH_FILTER_FACTORY, tableWithFilterFactory);

        Table<Bean> tableWithAccessRight = new Table<>(collection, TEST_RIGHT, false);
        tableRegistry.register(TABLE_WITH_ACCESS_RIGHT, tableWithAccessRight);

        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new ObjectHook() {
            @Override
            public ObjectFilter getObjectFilter(AbstractContext context) {
                return () -> "property1 = " + VALUE_2;
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
        TableService tableService = new TableService(tableRegistry, objectHookRegistry, new TestAccessManager());

        // Test empty request
        TableRequest request = new TableRequest();
        TableResponse<?> response = tableService.request(SIMPLE_TABLE, request, null);
        assertEquals(List.of(bean1, bean2, bean3), response.getData());
        assertEquals(3, response.getRecordsTotal());
        assertEquals(3, response.getRecordsFiltered());

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
        assertEquals(ENRICHED_ATTRIBUTE_VALUE, bean1.getAttribute(ENRICHED_ATTRIBUTE_KEY));

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

    private static class MyTableParameters extends TableParameters {

    }

    private static class TestAccessManager extends NoAccessManager {

        @Override
        public boolean checkRightInContext(Session session, String right) {
            return session != null && session.get(right) != null;
        }

    }
}

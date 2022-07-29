package step.framework.server.tables.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Test;
import step.core.AbstractContext;
import step.core.access.Role;
import step.core.access.RoleResolver;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.core.objectenricher.*;
import step.framework.server.Session;
import step.framework.server.access.AccessManager;
import step.framework.server.tables.Table;
import step.framework.server.tables.TableFindResult;
import step.framework.server.tables.TableRegistry;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TableServiceTest {

    private static final String TEST_RIGHT = "test-right";

    @Test
    public void request() throws TableServiceException {
        TableRegistry tableRegistry = new TableRegistry();
        TestTable table = getTable(false, false, null);
        tableRegistry.register("table1", table);

        TestTable table2 = getTable(true, false, null);
        tableRegistry.register("table2", table2);

        TestTable table3 = getTable(false, true, null);
        tableRegistry.register("table3", table3);

        TestTable tableWithRequiredAccessRight = getTable(false, true, TEST_RIGHT);
        tableRegistry.register("tableWithRequiredAccessRight", tableWithRequiredAccessRight);

        ObjectHookRegistry objectHookRegistry = new ObjectHookRegistry();
        objectHookRegistry.add(new ObjectHook() {
            @Override
            public ObjectFilter getObjectFilter(AbstractContext context) {
                return () -> "field2 = value2";
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

        Assert.assertThrows(TableServiceException.class, () -> tableService.request("invalid", null, null));

        TableRequest request = new TableRequest();
        tableService.request("table1", request, null);

        assertEquals(Filters.True.class, table.filter.getClass());

        request = new TableRequest();
        request.setFilters(List.of(new FieldFilter("field", "value", true)));

        TableResponse<?> response = tableService.request("table1", request, null);
        assertEquals(Filters.And.class, table.filter.getClass());
        assertEquals(1, table.filter.getChildren().size());
        Filters.Regex child = (Filters.Regex) table.filter.getChildren().get(0);
        assertEquals("field", child.getField());
        assertEquals("value", child.getExpression());
        assertEquals(0, response.getRecordsFiltered());
        assertEquals(0, response.getRecordsTotal());
        assertNotNull(response.getData());

        tableService.request("table2", request, null);
        assertEquals(Filters.And.class, table2.filter.getClass());
        assertEquals(2, table2.filter.getChildren().size());
        Filters.Equals childEquals = (Filters.Equals) table2.filter.getChildren().get(0);
        assertEquals("field2", childEquals.getField());
        assertEquals("value2", childEquals.getExpectedValue());

        tableService.request("table3", request, null);
        assertEquals(Filters.And.class, table3.filter.getClass());
        assertEquals(2, table3.filter.getChildren().size());
        childEquals = (Filters.Equals) table3.filter.getChildren().get(0);
        assertEquals("field3", childEquals.getField());
        assertEquals("value3", childEquals.getExpectedValue());

        request = new TableRequest();
        request.setFilters(List.of(new FieldFilter("field", "value", true)));
        request.setSkip(0);
        request.setLimit(10);
        Sort sort = new Sort();
        sort.setField("field1");
        sort.setDirection(SortDirection.DESCENDING);
        request.setSort(sort);

        tableService.request("table3", request, null);
        assertEquals(0, (int) table3.skip);
        assertEquals(10, (int) table3.limit);
        assertEquals("field1", table3.order.getAttributeName());
        assertEquals(-1, table3.order.getOrder());

        // Query a table requiring a right with the correct right
        request = new TableRequest();
        Session session = new Session();
        session.put(TEST_RIGHT, new Object());
        response = tableService.request("tableWithRequiredAccessRight", request, session);
        assertNotNull(response);

        // Query a table requiring a right without the required right in session
        assertThrows(TableServiceException.class, () -> tableService.request("tableWithRequiredAccessRight", new TableRequest(), new Session()));

        // Query a table requiring a right without session
        assertThrows(TableServiceException.class, () -> tableService.request("tableWithRequiredAccessRight", new TableRequest(), null));


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
        assertEquals(Filters.Not.class, oqlFilter_.getClass());

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

    private TestTable getTable(boolean filtered, boolean additionalQueryFragments, String requiredAccessRight) {
        return new TestTable(filtered, additionalQueryFragments, requiredAccessRight);
    }

    private static class TestTable implements Table<Object> {

        private final boolean filtered;
        private final boolean additionalQueryFragments;
        private Filter filter;
        private SearchOrder order;
        private Integer skip;
        private Integer limit;
        private String requiredAccessRight;

        public TestTable(boolean filtered, boolean additionalQueryFragments, String requiredAccessRight) {
            this.filtered = filtered;
            this.additionalQueryFragments = additionalQueryFragments;
            this.requiredAccessRight = requiredAccessRight;
        }

        @Override
        public TableFindResult<Object> find(Filter filter, SearchOrder order, Integer skip, Integer limit) {
            this.filter = filter;
            this.order = order;
            this.limit = limit;
            this.skip = skip;
            return new TableFindResult<>(0, 0, List.of().listIterator());
        }

        @Override
        public List<Filter> getTableFilters(TableParameters tableParameters) {
            return additionalQueryFragments ? List.of(Filters.equals("field3", "value3")) : null;
        }

        @Override
        public boolean isContextFiltered() {
            return filtered;
        }

        @Override
        public String getRequiredAccessRight() {
            return requiredAccessRight;
        }
    }

    private static class TestAccessManager implements AccessManager {
        @Override
        public void setRoleResolver(RoleResolver roleResolver) {

        }

        @Override
        public boolean checkRightInContext(Session session, String right) {
            return session != null && session.get(right) != null;
        }

        @Override
        public Role getRoleInContext(Session session) {
            return null;
        }
    }
}

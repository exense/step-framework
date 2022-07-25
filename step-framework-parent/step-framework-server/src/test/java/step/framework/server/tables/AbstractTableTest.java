package step.framework.server.tables;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.core.collections.inmemory.InMemoryCollection;
import step.core.entities.SimpleBean;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AbstractTableTest {

    private InMemoryCollection<Object> collection = new InMemoryCollection<>();

    @Test
    public void testFind() {
        AbstractTable<Object> table = new AbstractTable<>(collection, false);

        AbstractIdentifiableObject entity1 = new AbstractIdentifiableObject();
        collection.save(entity1);

        TableFindResult<Object> result = table.find(Filters.empty(), null, null, null);
        assertEquals(1, result.getRecordsTotal());
        assertEquals(1, result.getRecordsFiltered());
        assertEquals(entity1, result.getIterator().next());

        AbstractIdentifiableObject entity2 = new AbstractIdentifiableObject();
        collection.save(entity2);

        result = table.find(Filters.empty(), null, null, null);
        assertEquals(2, result.getRecordsTotal());
        assertEquals(2, result.getRecordsFiltered());
        assertEquals(entity1, result.getIterator().next());
    }

    @Test
    public void testFindWithSort() {
        AbstractTable<Object> table = new AbstractTable<>(collection, false);

        SimpleBean a = new SimpleBean("a");
        collection.save(a);
        SimpleBean b = new SimpleBean("b");
        collection.save(b);

        TableFindResult<Object> result = table.find(Filters.empty(), new SearchOrder("stringProperty", 1), null, null);
        assertEquals(List.of(a, b), IteratorUtils.toList(result.getIterator()));

        result = table.find(Filters.empty(), new SearchOrder("stringProperty", -1), null, null);
        assertEquals(List.of(b, a), IteratorUtils.toList(result.getIterator()));
    }

    @Test
    public void testFindWithSkipLimit() {
        AbstractTable<Object> table = new AbstractTable<>(collection, false);

        SimpleBean a = new SimpleBean("a");
        collection.save(a);
        SimpleBean b = new SimpleBean("b");
        collection.save(b);

        TableFindResult<Object> result = table.find(Filters.empty(), new SearchOrder("stringProperty", 1), 0, 1);
        assertEquals(List.of(a), IteratorUtils.toList(result.getIterator()));

        result = table.find(Filters.empty(), new SearchOrder("stringProperty", 1), 1, 2);
        assertEquals(List.of(b), IteratorUtils.toList(result.getIterator()));

        result = table.find(Filters.empty(), new SearchOrder("stringProperty", -1), 1, 2);
        assertEquals(List.of(a), IteratorUtils.toList(result.getIterator()));
    }

    @Test
    public void testDistinct() {
        AbstractTable<Object> table = new AbstractTable<>(collection, false);

        SimpleBean a = new SimpleBean("a");
        collection.save(a);
        SimpleBean b = new SimpleBean("b");
        collection.save(b);

        List<String> distinct = table.distinct("stringProperty");
        assertEquals(Set.of("a", "b"), new HashSet<>(distinct));

        distinct = table.distinct("stringProperty", Filters.equals("stringProperty", "a"));
        assertEquals(Set.of("a"), new HashSet<>(distinct));
    }
}
package step.core.collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FiltersSerializationTest {

    @Test
    public void testRegex() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.regex("myField", ".*", false);

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Regex\",\"field\":\"myField\",\"expression\":\".*\",\"caseSensitive\":false,\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Regex\",\"field\":\"myField\",\"expression\":\".*\",\"caseSensitive\":false,\"children\":null}", Filter.class);
        assertEquals(filter, parsedFilter);

        parsedFilter = reader.readValue("{\"type\":\"Regex\",\"field\":\"myField\",\"expression\":\".*\",\"caseSensitive\":false}", Filter.class);
        assertEquals(filter, parsedFilter);

        parsedFilter = reader.readValue("{\"type\":\"Regex\", \"field\":\"myField\", \"expression\":\".*\"}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testEquals() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.equals("myField", "myValue");

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Equals\",\"field\":\"myField\",\"expectedValue\":\"myValue\",\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Equals\",\"field\":\"myField\",\"expectedValue\":\"myValue\",\"children\":null}", Filter.class);
        assertEquals(filter, parsedFilter);

        parsedFilter = reader.readValue("{\"type\":\"Equals\",\"field\":\"myField\",\"expectedValue\":\"myValue\"}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testFulltext() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.fulltext("myFullttextExpression");

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Fulltext\",\"expression\":\"myFullttextExpression\",\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Fulltext\",\"expression\":\"myFullttextExpression\"}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testGt() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.gt("myField", 0);

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Gt\",\"field\":\"myField\",\"value\":0,\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Gt\",\"field\":\"myField\",\"value\":0}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testGte() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.gte("myField", 0);

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Gte\",\"field\":\"myField\",\"value\":0,\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Gte\",\"field\":\"myField\",\"value\":0}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testLt() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.lt("myField", 0);

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Lt\",\"field\":\"myField\",\"value\":0,\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Lt\",\"field\":\"myField\",\"value\":0}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testLte() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.lte("myField", 0);

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Lte\",\"field\":\"myField\",\"value\":0,\"children\":null}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Lte\",\"field\":\"myField\",\"value\":0}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testAnd() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.and(List.of(Filters.empty(), Filters.empty()));

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"And\",\"children\":[{\"type\":\"True\",\"children\":null},{\"type\":\"True\",\"children\":null}]}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"And\",\"children\":[{\"type\":\"True\"},{\"type\":\"True\"}]}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testOr() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.or(List.of(Filters.empty(), Filters.empty()));

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Or\",\"children\":[{\"type\":\"True\",\"children\":null},{\"type\":\"True\",\"children\":null}]}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Or\",\"children\":[{\"type\":\"True\"},{\"type\":\"True\"}]}", Filter.class);
        assertEquals(filter, parsedFilter);
    }

    @Test
    public void testNot() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Filter filter = Filters.not(Filters.falseFilter());

        ObjectWriter writer = objectMapper.writer();
        String filterAsJson = writer.writeValueAsString(filter);
        assertEquals("{\"type\":\"Not\",\"children\":[{\"type\":\"False\",\"children\":null}]}", filterAsJson);

        ObjectReader reader = objectMapper.reader();
        Filter parsedFilter = reader.readValue("{\"type\":\"Not\",\"children\":[{\"type\":\"False\"}]}", Filter.class);
        assertEquals(filter, parsedFilter);
    }
}
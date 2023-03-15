package step.core.timeseries;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.timeseries.query.OQLTimeSeriesFilterBuilder;

import java.util.Arrays;
import java.util.List;

public class OQLTimeSeriesFilterBuilderTest {

    @Test
    public void emptyOqlTest() {
        String oql = "";
        Filter filter = OQLTimeSeriesFilterBuilder.getFilter(oql);
        Assert.assertEquals(filter, Filters.empty());
        oql = null;
        filter = OQLTimeSeriesFilterBuilder.getFilter(oql);
        Assert.assertEquals(filter, Filters.empty());
    }

    @Test
    public void simpleOqlTest() {
        String oql = "(field1 = 5 and field2 > 8)";
        Filter filter = OQLTimeSeriesFilterBuilder.getFilter(oql);
        Assert.assertEquals(2, filter.getChildren().size());
    }

    @Test
    public void filterAttributesTest() {
        String oql = "(field1 = 5 and field2 > 8 or (field3 < 1 or field4 > 4))";
        List<String> filterAttributes = OQLTimeSeriesFilterBuilder.getFilterAttributes(oql);
        Assert.assertEquals(4, filterAttributes.size());
        Assert.assertTrue(filterAttributes.containsAll(Arrays.asList("field1", "field2", "field3", "field4")));
    }

    @Test(expected = IllegalStateException.class)
    public void invalidOqlTest() {
        String oql = "field5)";
        OQLTimeSeriesFilterBuilder.getFilterAttributes(oql);
    }


}

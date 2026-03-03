package step.core.ql;

import org.antlr.v4.runtime.*;
import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filter;
import step.core.collections.filters.Equals;
import step.core.collections.filters.In;
import step.core.collections.filters.True;

import java.util.Arrays;
import java.util.HashSet;

public class OQLAttributesAwareFilterVisitorTest {

    @Test
    public void baseVisitorTest() {
        OQLParser.ParseContext context = parse("field1 = 123");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor();
        Filter filter = visitor.visit(context.getChild(0));
        Assert.assertTrue(filter instanceof Equals);
        Assert.assertEquals("field1", ((Equals) filter).getField());
        Assert.assertEquals(1, visitor.getAttributes().size());
        Assert.assertEquals("field1", visitor.getAttributes().get(0));
    }

    @Test
    public void nestedAttributesTest() {
        OQLParser.ParseContext context = parse("((field1 ~ 123) or (field1 = abc)) and (field2 = 5 and not (field3 < 5 and field4 != abc))");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor();
        visitor.visit(context.getChild(0));
        Assert.assertEquals(5, visitor.getAttributes().size());
        Assert.assertEquals(4, new HashSet<>(visitor.getAttributes()).size());
        Assert.assertTrue(visitor.getAttributes().containsAll(Arrays.asList("field1", "field2", "field3", "field4")));
    }

    @Test
    public void simpleFilterTransformTest() {
        OQLParser.ParseContext context = parse("field1 = 123");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor((key) -> "prefix." + key);
        Filter filter = visitor.visit(context.getChild(0));
        Assert.assertTrue(filter instanceof Equals);
        Assert.assertEquals("prefix.field1", ((Equals) filter).getField());
        Assert.assertEquals(1, visitor.getAttributes().size());
        Assert.assertEquals("prefix.field1", visitor.getAttributes().get(0));
    }

    @Test
    public void transformWithIgnoredFieldsTest() {
        OQLParser.ParseContext context = parse("field1 = 123");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor((key) -> "prefix." + key, Arrays.asList("field1"));
        Filter filter = visitor.visit(context.getChild(0));
        Assert.assertTrue(filter instanceof Equals);
        Assert.assertEquals("prefix.field1", ((Equals) filter).getField());
        Assert.assertEquals(1, visitor.getAttributes().size());
        Assert.assertEquals("prefix.field1", visitor.getAttributes().get(0));

        visitor = new OQLAttributesAwareFilterVisitor((key) -> "prefix." + key, Arrays.asList("prefix.field1"));
        filter = visitor.visit(context.getChild(0));
        Assert.assertTrue(filter instanceof True);
        Assert.assertTrue(visitor.getAttributes().isEmpty());
    }

    @Test
    public void transformWithInCriteriaTest() {
        OQLParser.ParseContext context = parse("field1 in ( \"prop1\", \"prop2\" )");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor((key) -> "prefix." + key);
        Filter filter = visitor.visit(context.getChild(0));
        Assert.assertTrue(filter instanceof In);
        In inFilter = (In) filter;
        Assert.assertEquals(2, inFilter.getValues().size());
        Assert.assertEquals("prefix.field1", inFilter.getField());
        Assert.assertEquals("prop1", inFilter.getValues().get(0));
        Assert.assertEquals("prop2", inFilter.getValues().get(1));
    }

    private static OQLParser.ParseContext parse(String expression) {
        OQLLexer lexer = new OQLLexer(new ANTLRInputStream(expression));
        OQLParser parser = new OQLParser(new CommonTokenStream(lexer));
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new IllegalStateException("failed to parse at line " + line + " due to " + msg, e);
            }
        });
        return parser.parse();
    }
}

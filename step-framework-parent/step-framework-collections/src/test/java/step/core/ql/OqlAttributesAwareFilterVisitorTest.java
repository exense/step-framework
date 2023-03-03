package step.core.ql;

import org.antlr.v4.runtime.*;
import org.junit.Assert;
import org.junit.Test;
import step.core.collections.Filter;
import step.core.collections.filters.Equals;

import java.util.Arrays;

public class OqlAttributesAwareFilterVisitorTest {

    @Test
    public void baseVisitorTest() {
        OQLParser.ParseContext context = parse("field1 = 123");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor();
        Filter filter = visitor.visit(context.getChild(0));
        Assert.assertTrue(filter instanceof Equals);
        Assert.assertEquals(1, visitor.getAttributes().size());
        Assert.assertEquals("field1", visitor.getAttributes().get(0));
    }

    @Test
    public void nestedAttributesTest() {
        OQLParser.ParseContext context = parse("(field1 = 123) and (field2 = 5 and (field3 < 5 and field4 = abc))");
        OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor();
        visitor.visit(context.getChild(0));
        Assert.assertEquals(4, visitor.getAttributes().size());
        Assert.assertTrue(visitor.getAttributes().containsAll(Arrays.asList("field1", "field2", "field3", "field4")));
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

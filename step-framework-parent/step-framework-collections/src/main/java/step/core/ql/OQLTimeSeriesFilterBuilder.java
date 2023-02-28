package step.core.ql;

import org.antlr.v4.runtime.*;
import step.core.collections.*;

import java.util.*;
import java.util.Collection;
import java.util.function.Function;

public class OQLTimeSeriesFilterBuilder {

    public static Filter getFilter(String expression,
                                   Function<String, String> attributesTransformFunction,
                                   Collection<String> ignoreAttributes) {
        if (expression == null || expression.isEmpty()) {
            return Filters.empty();
        } else {
            OQLParser.ParseContext context = parse(expression);
            OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor(attributesTransformFunction, ignoreAttributes);
            return visitor.visit(context.getChild(0));
        }
    }

    public static void main(String[] args) {
        String s = "((attributes.mField >= 30))";
        System.out.println(OQLTimeSeriesFilterBuilder.getFilter(s));
    }

    public static Filter getFilter(String expression) {
        return getFilter(expression, null, Collections.emptySet());
    }

    public static List<String> getFilterAttributes(String expression) {
        if (expression == null || expression.isEmpty()) {
            return Collections.emptyList();
        } else {
            OQLParser.ParseContext context = parse(expression);
            OQLAttributesAwareFilterVisitor visitor = new OQLAttributesAwareFilterVisitor();
            Filter visit = visitor.visit(context.getChild(0));
            return visitor.getAttributes();
        }
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

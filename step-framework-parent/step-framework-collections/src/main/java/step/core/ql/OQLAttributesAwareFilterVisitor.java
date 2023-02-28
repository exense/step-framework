package step.core.ql;

import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OQLAttributesAwareFilterVisitor extends OQLBaseVisitor<Filter> {

    private List<String> attributes = new ArrayList<>();
    private Function<String, String> attributesTransformFunction;
    private Collection<String> ignoreAttributes;

    public OQLAttributesAwareFilterVisitor() {
        this(Function.identity(), Collections.emptySet());
    }

    /**
     * Transform is the first operation, then ignore attributes
     * @param attributesTransformFunction
     * @param ignoreAttributes
     */
    public OQLAttributesAwareFilterVisitor(Function<String, String> attributesTransformFunction, Collection<String> ignoreAttributes) {
        this.attributesTransformFunction = attributesTransformFunction != null ? attributesTransformFunction : Function.identity();
        this.ignoreAttributes = ignoreAttributes;
    }

    @Override
    public Filter visitAndExpr(OQLParser.AndExprContext ctx) {
        final Filter left = this.visit(ctx.expr(0));
        final Filter right = this.visit(ctx.expr(1));
        return Filters.and(List.of(left, right));
    }

    @Override
    public Filter visitEqualityExpr(OQLParser.EqualityExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        attributes.add(text0);
        String text1 = unescapeStringIfNecessary(ctx.expr(1).getText());
        if (ctx.EQ() != null) {
            return Filters.equals(text0, text1);
        } else if (ctx.NEQ() != null) {
            return Filters.not(Filters.equals(text0, text1));
        } else if (ctx.REGEX() != null) {
            return Filters.regex(text0, text1,false);
        } else {
            throw new UnsupportedOperationException("Operation of the provided equality expression is not supported. Expression: " + ctx.getText());
        }
    }

    @Override
    public Filter visitComparisonExpr(OQLParser.ComparisonExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        attributes.add(text0);
        String text1 = unescapeStringIfNecessary(ctx.expr(1).getText());
        Long value;
        try {
            value = Long.parseLong(text1);
        } catch (Exception e) {
            throw new UnsupportedOperationException("Comparison expression only support long value. Expression: " + ctx.getText());
        }
        if (ctx.LT() != null) {
            return Filters.lt(text0, value);
        } else if (ctx.LTE() != null) {
            return Filters.lte(text0, value);
        } else if (ctx.GT() != null) {
            return Filters.gt(text0, value);
        } else if (ctx.GTE() != null) {
            return Filters.gte(text0, value);
        } else {
            throw new UnsupportedOperationException("Operation of the provided comparison expression is not supported. Expression: " + ctx.getText());
        }
    }

    protected String unescapeStringIfNecessary(String text1) {
        if(text1.startsWith("\"") && text1.endsWith("\"")) {
            text1 = unescapeStringAtom(text1);
        }
        return text1;
    }

    @Override
    public Filter visitOrExpr(OQLParser.OrExprContext ctx) {
        final Filter left = this.visit(ctx.expr(0));
        final Filter right = this.visit(ctx.expr(1));
        return Filters.or(List.of(left, right));
    }

    @Override
    public Filter visitInExpr(OQLParser.InExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr().getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        attributes.add(text0);
        List<String> ins = ctx.STRING().stream().map(tn -> unescapeStringIfNecessary(tn.getText())).collect(Collectors.toList());
        return Filters.in(text0, ins);
    }

    @Override
    public Filter visitNotExpr(OQLParser.NotExprContext ctx) {
        final Filter expr = this.visit(ctx.expr());
        return Filters.not(expr);
    }

    @Override
    public Filter visitParExpr(OQLParser.ParExprContext ctx) {
        return this.visit(ctx.expr());
    }

    @Override
    public Filter visitNonQuotedStringAtom(OQLParser.NonQuotedStringAtomContext ctx) {
        return Filters.fulltext(ctx.getText());
    }

    @Override
    public Filter visitStringAtom(OQLParser.StringAtomContext ctx) {
        String str = unescapeStringAtom(ctx.getText());
        return Filters.fulltext(str);
    }

    protected String unescapeStringAtom(String str) {
        // strip quotes
        str = str.substring(1, str.length() - 1).replace("\"\"", "\"");
        return str;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    private String transform(String attribute) {
        return this.attributesTransformFunction.apply(attribute);
    }
}

package step.core.ql;

import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OQLAttributesAwareFilterVisitor extends OQLFilterVisitor {

    private final List<String> attributes = new ArrayList<>();
    private final Function<String, String> attributesTransformFunction;
    private final Collection<String> ignoreAttributes;

    public OQLAttributesAwareFilterVisitor() {
        this(Function.identity(), Collections.emptySet());
    }

    public OQLAttributesAwareFilterVisitor(Function<String, String> attributesTransformFunction) {
        this(attributesTransformFunction, Collections.emptySet());
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
    public Filter visitEqualityExpr(OQLParser.EqualityExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        String text1 = unescapeStringIfNecessary(ctx.expr(1).getText());
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        attributes.add(text0);
        return processEqExpr(ctx, text0, text1);
    }

    @Override
    public Filter visitComparisonExpr(OQLParser.ComparisonExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        attributes.add(text0);
        String text1 = unescapeStringIfNecessary(ctx.expr(1).getText());
        return processComparisonExp(ctx, text0, text1);
    }

    @Override
    public Filter visitInExpr(OQLParser.InExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr().getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        attributes.add(text0);
        return processInExpr(ctx, text0);
    }

    public List<String> getAttributes() {
        return attributes;
    }

    private String transform(String attribute) {
        return this.attributesTransformFunction.apply(attribute);
    }
}

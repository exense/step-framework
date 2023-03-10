package step.core.ql;

import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class OQLAttributesAwareFilterVisitor extends OQLFilterVisitor {

    private final List<String> attributes = new ArrayList<>();
    private final Function<String, String> attributesTransformFunction;
    private final Collection<String> ignoreAttributes;

    public OQLAttributesAwareFilterVisitor() {
        this(Function.identity(), Collections.emptySet());
    }

    /**
     * Transform is the first operation, then ignore attributes
     *
     * @param attributesTransformFunction
     * @param ignoreAttributes
     */
    public OQLAttributesAwareFilterVisitor(Function<String, String> attributesTransformFunction, Collection<String> ignoreAttributes) {
        super();
        this.attributesTransformFunction = attributesTransformFunction != null ? attributesTransformFunction : Function.identity();
        this.ignoreAttributes = ignoreAttributes;
    }

    @Override
    public Filter visitEqualityExpr(OQLParser.EqualityExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        trackAttribute(text0);
        return super.visitEqualityExpr(ctx);
    }

    private void trackAttribute(String text0) {
        attributes.add(text0);
    }

    @Override
    public Filter visitComparisonExpr(OQLParser.ComparisonExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        trackAttribute(text0);
        return super.visitComparisonExpr(ctx);
    }

    @Override
    public Filter visitInExpr(OQLParser.InExprContext ctx) {
        String text0 = transform(unescapeStringIfNecessary(ctx.expr().getText()));
        if (ignoreAttributes.contains(text0)) {
            return Filters.empty();
        }
        trackAttribute(text0);
        return super.visitInExpr(ctx);
    }

    public List<String> getAttributes() {
        return attributes;
    }

    private String transform(String attribute) {
        return this.attributesTransformFunction.apply(attribute);
    }
}

package step.core.ql;

import step.core.collections.Filter;
import step.core.collections.Filters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * An {@link OQLFilterVisitor} that tracks which attributes appear in the parsed OQL expression
 * and optionally suppresses filtering on a declared set of attributes.
 *
 * <p>When a filter expression (equality, comparison, {@code IN}, or {@code includes}) references
 * an attribute listed in {@code ignoreAttributes}, the expression is replaced with
 * {@link Filters#empty()} (a match-all predicate) instead of being translated into a real filter.
 * This makes the clause a no-op without altering the surrounding logical structure of the query.
 *
 * <p>Attribute names are passed through {@code attributesTransformFunction} before the
 * ignore-list check is applied.
 */
public class OQLAttributesAwareFilterVisitor extends OQLFilterVisitor {

    private final List<String> attributes = new ArrayList<>();
    private final Function<String, String> attributesTransformFunction;
    /**
     * Attribute names for which filter expressions should be bypassed. Any OQL clause that
     * references one of these attributes is replaced with a match-all ({@link Filters#empty()})
     * rather than being evaluated, effectively removing that constraint from the query.
     * The check is performed after {@code attributesTransformFunction} has been applied.
     */
    private final Collection<String> ignoreAttributes;

    public OQLAttributesAwareFilterVisitor() {
        this(Function.identity(), Collections.emptySet());
    }

    public OQLAttributesAwareFilterVisitor(Function<String, String> attributesTransformFunction) {
        this(attributesTransformFunction, Collections.emptySet());
    }


    /**
     * @param attributesTransformFunction function applied to every attribute name before it is
     *                                    used as a filter field or checked against
     *                                    {@code ignoreAttributes}; {@code null} is treated as the
     *                                    identity function
     * @param ignoreAttributes            attribute names whose filter expressions should be
     *                                    replaced with a match-all predicate; the check is
     *                                    performed after the transform has been applied
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

    @Override
    public Filter visitIncludesExpr(OQLParser.IncludesExprContext ctx) {
        String field = transform(unescapeStringIfNecessary(ctx.expr(0).getText()));
        if (ignoreAttributes.contains(field)) {
            return Filters.empty();
        }
        attributes.add(field);
        String value = unescapeStringIfNecessary(ctx.expr(1).getText());
        return processIncludesExpr(field, value);
    }

    public List<String> getAttributes() {
        return attributes;
    }

    private String transform(String attribute) {
        return this.attributesTransformFunction.apply(attribute);
    }
}

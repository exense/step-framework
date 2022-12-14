/*******************************************************************************
 * Copyright (C) 2020, exense GmbH
 *  
 * This file is part of STEP
 *  
 * STEP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *  
 * STEP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *  
 * You should have received a copy of the GNU Affero General Public License
 * along with STEP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package step.core.ql;

import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.ql.OQLParser.*;

import java.util.List;

public class OQLFilterVisitor extends OQLBaseVisitor<Filter>{

	public OQLFilterVisitor() {
		super();
	}

	@Override
	public Filter visitAndExpr(AndExprContext ctx) {
		final Filter left = this.visit(ctx.expr(0));
		final Filter right = this.visit(ctx.expr(1));
        return Filters.and(List.of(left, right));
	}

	@Override
	public Filter visitEqualityExpr(EqualityExprContext ctx) {
		String text0 = unescapeStringIfNecessary(ctx.expr(0).getText());
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
	public Filter visitComparisonExpr(ComparisonExprContext ctx) {
		String text0 = unescapeStringIfNecessary(ctx.expr(0).getText());
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
	public Filter visitOrExpr(OrExprContext ctx) {
		final Filter left = this.visit(ctx.expr(0));
		final Filter right = this.visit(ctx.expr(1));
        return Filters.or(List.of(left, right));
	}

	@Override
	public Filter visitNotExpr(NotExprContext ctx) {
		final Filter expr = this.visit(ctx.expr());
        return Filters.not(expr);
	}

	@Override
	public Filter visitParExpr(ParExprContext ctx) {
		return this.visit(ctx.expr());
	}

	@Override
	public Filter visitNonQuotedStringAtom(NonQuotedStringAtomContext ctx) {
		return Filters.fulltext(ctx.getText());
	}

	@Override
	public Filter visitStringAtom(StringAtomContext ctx) {
		String str = unescapeStringAtom(ctx.getText());
        return Filters.fulltext(str);
	}

	protected String unescapeStringAtom(String str) {
        // strip quotes
        str = str.substring(1, str.length() - 1).replace("\"\"", "\"");
		return str;
	}


}

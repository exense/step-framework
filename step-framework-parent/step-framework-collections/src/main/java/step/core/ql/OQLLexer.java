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
// Generated from OQL.g4 by ANTLR 4.5.3

    package step.core.ql;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class OQLLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		EQ=1, NEQ=2, REGEX=3, OR=4, AND=5, NOT=6, LT=7, LTE=8, GT=9, GTE=10, OPAR=11, 
		CPAR=12, NONQUOTEDSTRING=13, STRING=14, SPACE=15;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"EQ", "NEQ", "REGEX", "OR", "AND", "NOT", "LT", "LTE", "GT", "GTE", "OPAR", 
		"CPAR", "NONQUOTEDSTRING", "STRING", "SPACE"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'='", "'!='", "'~'", "'or'", "'and'", "'not'", "'<'", "'<='", "'>'", 
		"'>='", "'('", "')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "EQ", "NEQ", "REGEX", "OR", "AND", "NOT", "LT", "LTE", "GT", "GTE", 
		"OPAR", "CPAR", "NONQUOTEDSTRING", "STRING", "SPACE"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public OQLLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "OQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\21U\b\1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\3\2\3\2\3\3\3\3\3\3"+
		"\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\b\3\b\3\t\3\t\3"+
		"\t\3\n\3\n\3\13\3\13\3\13\3\f\3\f\3\r\3\r\3\16\6\16C\n\16\r\16\16\16D"+
		"\3\17\3\17\3\17\3\17\7\17K\n\17\f\17\16\17N\13\17\3\17\3\17\3\20\3\20"+
		"\3\20\3\20\2\2\21\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31"+
		"\16\33\17\35\20\37\21\3\2\5\n\2&&/\60\62;>>@@C\\aac|\5\2\f\f\17\17$$\5"+
		"\2\13\f\17\17\"\"W\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13"+
		"\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2"+
		"\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\3"+
		"!\3\2\2\2\5#\3\2\2\2\7&\3\2\2\2\t(\3\2\2\2\13+\3\2\2\2\r/\3\2\2\2\17\63"+
		"\3\2\2\2\21\65\3\2\2\2\238\3\2\2\2\25:\3\2\2\2\27=\3\2\2\2\31?\3\2\2\2"+
		"\33B\3\2\2\2\35F\3\2\2\2\37Q\3\2\2\2!\"\7?\2\2\"\4\3\2\2\2#$\7#\2\2$%"+
		"\7?\2\2%\6\3\2\2\2&\'\7\u0080\2\2\'\b\3\2\2\2()\7q\2\2)*\7t\2\2*\n\3\2"+
		"\2\2+,\7c\2\2,-\7p\2\2-.\7f\2\2.\f\3\2\2\2/\60\7p\2\2\60\61\7q\2\2\61"+
		"\62\7v\2\2\62\16\3\2\2\2\63\64\7>\2\2\64\20\3\2\2\2\65\66\7>\2\2\66\67"+
		"\7?\2\2\67\22\3\2\2\289\7@\2\29\24\3\2\2\2:;\7@\2\2;<\7?\2\2<\26\3\2\2"+
		"\2=>\7*\2\2>\30\3\2\2\2?@\7+\2\2@\32\3\2\2\2AC\t\2\2\2BA\3\2\2\2CD\3\2"+
		"\2\2DB\3\2\2\2DE\3\2\2\2E\34\3\2\2\2FL\7$\2\2GK\n\3\2\2HI\7$\2\2IK\7$"+
		"\2\2JG\3\2\2\2JH\3\2\2\2KN\3\2\2\2LJ\3\2\2\2LM\3\2\2\2MO\3\2\2\2NL\3\2"+
		"\2\2OP\7$\2\2P\36\3\2\2\2QR\t\4\2\2RS\3\2\2\2ST\b\20\2\2T \3\2\2\2\6\2"+
		"DJL\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
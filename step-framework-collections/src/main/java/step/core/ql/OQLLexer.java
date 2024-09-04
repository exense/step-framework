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
		EQ=1, NEQ=2, REGEX=3, OR=4, AND=5, IN=6, NOT=7, LT=8, LTE=9, GT=10, GTE=11, 
		OPAR=12, CPAR=13, COMMA=14, NONQUOTEDSTRING=15, STRING=16, SPACE=17;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"EQ", "NEQ", "REGEX", "OR", "AND", "IN", "NOT", "LT", "LTE", "GT", "GTE", 
		"OPAR", "CPAR", "COMMA", "NONQUOTEDSTRING", "STRING", "SPACE"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'='", "'!='", "'~'", "'or'", "'and'", "'in'", "'not'", "'<'", "'<='", 
		"'>'", "'>='", "'('", "')'", "','"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "EQ", "NEQ", "REGEX", "OR", "AND", "IN", "NOT", "LT", "LTE", "GT", 
		"GTE", "OPAR", "CPAR", "COMMA", "NONQUOTEDSTRING", "STRING", "SPACE"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\23^\b\1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3"+
		"\b\3\b\3\b\3\b\3\t\3\t\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\r\3\r\3\16"+
		"\3\16\3\17\3\17\3\20\6\20L\n\20\r\20\16\20M\3\21\3\21\3\21\3\21\7\21T"+
		"\n\21\f\21\16\21W\13\21\3\21\3\21\3\22\3\22\3\22\3\22\2\2\23\3\3\5\4\7"+
		"\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22"+
		"#\23\3\2\5\n\2&&/\60\62;>>@@C\\aac|\5\2\f\f\17\17$$\5\2\13\f\17\17\"\""+
		"`\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2"+
		"\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2"+
		"\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2"+
		"\2\2\3%\3\2\2\2\5\'\3\2\2\2\7*\3\2\2\2\t,\3\2\2\2\13/\3\2\2\2\r\63\3\2"+
		"\2\2\17\66\3\2\2\2\21:\3\2\2\2\23<\3\2\2\2\25?\3\2\2\2\27A\3\2\2\2\31"+
		"D\3\2\2\2\33F\3\2\2\2\35H\3\2\2\2\37K\3\2\2\2!O\3\2\2\2#Z\3\2\2\2%&\7"+
		"?\2\2&\4\3\2\2\2\'(\7#\2\2()\7?\2\2)\6\3\2\2\2*+\7\u0080\2\2+\b\3\2\2"+
		"\2,-\7q\2\2-.\7t\2\2.\n\3\2\2\2/\60\7c\2\2\60\61\7p\2\2\61\62\7f\2\2\62"+
		"\f\3\2\2\2\63\64\7k\2\2\64\65\7p\2\2\65\16\3\2\2\2\66\67\7p\2\2\678\7"+
		"q\2\289\7v\2\29\20\3\2\2\2:;\7>\2\2;\22\3\2\2\2<=\7>\2\2=>\7?\2\2>\24"+
		"\3\2\2\2?@\7@\2\2@\26\3\2\2\2AB\7@\2\2BC\7?\2\2C\30\3\2\2\2DE\7*\2\2E"+
		"\32\3\2\2\2FG\7+\2\2G\34\3\2\2\2HI\7.\2\2I\36\3\2\2\2JL\t\2\2\2KJ\3\2"+
		"\2\2LM\3\2\2\2MK\3\2\2\2MN\3\2\2\2N \3\2\2\2OU\7$\2\2PT\n\3\2\2QR\7$\2"+
		"\2RT\7$\2\2SP\3\2\2\2SQ\3\2\2\2TW\3\2\2\2US\3\2\2\2UV\3\2\2\2VX\3\2\2"+
		"\2WU\3\2\2\2XY\7$\2\2Y\"\3\2\2\2Z[\t\4\2\2[\\\3\2\2\2\\]\b\22\2\2]$\3"+
		"\2\2\2\6\2MSU\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
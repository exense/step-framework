grammar OQL;

@header {
    package step.core.ql;
}

parse
    : expr? EOF
    ;
    
expr
 : NOT expr                             #notExpr
 | expr op=(EQ | NEQ | REGEX) expr              #equalityExpr
 | expr op=(LT | LTE | GT | GTE) expr           #comparisonExpr
 | expr AND expr                        #andExpr
 | expr OR expr                         #orExpr
 | expr IN OPAR (STRING (COMMA STRING)* )? CPAR            #inExpr
 | expr IS NOT NULL_LITERAL             #isNotNullExpr
 | expr IS NULL_LITERAL                 #isNullExpr
 | atom                                 #atomExpr
 ;    

atom
 : OPAR expr CPAR  #parExpr
 | NONQUOTEDSTRING #nonQuotedStringAtom
 | STRING          #stringAtom
 ;

EQ : '=';
NEQ : '!=';
REGEX : '~';
OR : 'or';
AND : 'and';
IN : 'in';
IS : 'is';
NOT : 'not';
NULL_LITERAL : 'null';
LT : '<';
LTE : '<=';
GT : '>';
GTE : '>=';

OPAR : '(';
CPAR : ')';
COMMA: ',';

NONQUOTEDSTRING: ('a'..'z' | 'A'..'Z' | '0'..'9' |'.'|'$'|'_'|'-'|'<'|'>')+ ;

STRING
 : '"' (~["\r\n] | '""')* '"'
 ;

SPACE
 : [ \t\r\n] -> skip
 ;
grammar Sql;

sqlStatement
    : statement (';')? EOF
    ;

statement
    : INSERT INTO path insertColumnsSpec VALUES insertValuesSpec #insertStatement
    | DELETE FROM path (COMMA path)* whereClause? #deleteStatement
    | selectClause fromClause whereClause? specialClause? #selectStatement
    | COUNT POINTS #countPointsStatement
    | DELETE TIME SERIES path (COMMA path)* #deleteTimeSeriesStatement
    | CLEAR DATA #clearDataStatement
    | SHOW TIME SERIES #showTimeSeriesStatement
    | SHOW REPLICA NUMBER #showReplicationStatement
    | ADD STORAGEENGINE storageEngineSpec #addStorageEngineStatement
    | SHOW CLUSTER INFO #showClusterInfoStatement
    ;

selectClause
   : SELECT expression (COMMA expression)*
   ;

expression
    : functionName LR_BRACKET path RR_BRACKET
    | path
    ;

functionName
    : ID
    | LAST
    | FIRST_VALUE
    | LAST_VALUE
    | MIN
    | MAX
    | AVG
    | COUNT
    | SUM
    ;

whereClause
    : WHERE orExpression
    ;

orExpression
    : andExpression (OPERATOR_OR andExpression)*
    ;

andExpression
    : predicate (OPERATOR_AND predicate)*
    ;

predicate
    : (TIME | TIMESTAMP | path) comparisonOperator constant
    | constant comparisonOperator (TIME | TIMESTAMP | path)
    | OPERATOR_NOT? LR_BRACKET orExpression RR_BRACKET
    ;

fromClause
    : FROM path
    ;

specialClause
    : limitClause
    | groupByTimeClause limitClause?
    | orderByClause limitClause?
    ;

orderByClause : ORDER BY (TIME | TIMESTAMP | path) (DESC | ASC)?;

groupByTimeClause
    : GROUP timeInterval BY DURATION
    ;

timeInterval
    : LS_BRACKET startTime=timeValue COMMA endTime=timeValue RR_BRACKET
    | LR_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    | LR_BRACKET startTime=timeValue COMMA endTime=timeValue RR_BRACKET
    | LS_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    ;

limitClause
    : LIMIT INT COMMA INT
    | LIMIT INT offsetClause?
    | offsetClause? LIMIT INT
    ;

offsetClause
    : OFFSET INT
    ;

comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
    ;

insertColumnsSpec
    : LR_BRACKET (TIMESTAMP|TIME) (COMMA measurementName)+ RR_BRACKET
    ;

measurementName
    : nodeName
    | LR_BRACKET nodeName (COMMA nodeName)+ RR_BRACKET
    ;

insertValuesSpec
    : (COMMA? insertMultiValue)*
    ;

insertMultiValue
    : LR_BRACKET timeValue (COMMA constant)+ RR_BRACKET
    ;

storageEngineSpec
    : (COMMA? storageEngine)+
    ;

storageEngine
    : LR_BRACKET ip COMMA port=INT COMMA engineType=stringLiteral COMMA extra=stringLiteral RR_BRACKET
    ;

timeValue
    : dateFormat
    | dateExpression
    | INT
    | MINUS? INF
    ;

path
    : nodeName (DOT nodeName)*
    ;

nodeName
    : ID
    | STAR
    | DOUBLE_QUOTE_STRING_LITERAL
    | DURATION
    | dateExpression
    | dateFormat
    | MINUS? (EXPONENT | INT)
    | booleanClause
    | INSERT
    | DELETE
    | SELECT
    | SHOW
    | INTO
    | WHERE
    | FROM
    | BY
    | LIMIT
    | OFFSET
    | TIME
    | SERIES
    | TIMESTAMP
    | GROUP
    | ORDER
    | ADD
    | VALUE
    | VALUES
    | NOW
    | COUNT
    | LAST
    | CLEAR
    | MIN
    | MAX
    | AVG
    | COUNT
    | SUM
    | DESC
    | ASC
    | STORAGEENGINE
    | POINTS
    | DATA
    | NULL
    | SHOW
    | REPLICA
    | IOTDB
    | INFLUXDB
    ;

ip
    : INT (DOT INT)*
    ;

dateFormat
    : DATETIME
    | NOW LR_BRACKET RR_BRACKET
    ;

constant
    : dateExpression
    | NaN
    | MINUS? realLiteral // double
    | MINUS? INT         // long
    | MINUS? FLOAT       // float
    | MINUS? INTEGER     // int
    | stringLiteral
    | booleanClause
    | NULL
    ;

booleanClause
    : TRUE
    | FALSE
    ;

dateExpression
    : dateFormat ((PLUS | MINUS) DURATION)*
    ;

realLiteral
    : INT DOT (INT | EXPONENT)?
    | DOT  (INT|EXPONENT)
    | EXPONENT
    ;

//============================
// Start of the keywords list
//============================
INSERT
    : I N S E R T
    ;

DELETE
    : D E L E T E
    ;

SELECT
    : S E L E C T
    ;

SHOW
    : S H O W
    ;

REPLICA
    : R E P L I C A
    ;

NUMBER
    : N U M B E R
    ;

CLUSTER
    : C L U S T E R
    ;

INFO
    : I N F O
    ;

WHERE
    : W H E R E
    ;

IN
    : I N
    ;

INTO
    : I N T O
    ;

FROM
    : F R O M
    ;

TIMESTAMP
    : T I M E S T A M P
    ;

GROUP
    : G R O U P
    ;

ORDER
    : O R D E R
    ;

BY
    : B Y
    ;

VALUE
    : V A L U E
    ;

VALUES
    : V A L U E S
    ;

IOTDB
    : I O T D B
    ;

INFLUXDB
    : I N F L U X D B
    ;

NOW
    : N O W
    ;

TIME
    : T I M E
    ;

TRUE
    : T R U E
    ;

FALSE
    : F A L S E
    ;

NULL
    : N U L L
    ;

LAST
    : L A S T
    ;

FIRST_VALUE
    : F I R S T '_' V A L U E
    ;

LAST_VALUE
    : L A S T '_' V A L U E
    ;

MIN
    : M I N
    ;

MAX
    : M A X
    ;

AVG
    : A V G
    ;

COUNT
    : C O U N T
    ;

SUM
    : S U M
    ;

LIMIT
    : L I M I T
    ;

OFFSET
    : O F F S E T
    ;

DATA
    : D A T A
    ;

ADD
    : A D D
    ;

STORAGEENGINE
    : S T O R A G E E N G I N E
    ;

POINTS
    : P O I N T S
    ;

CLEAR
    : C L E A R
    ;

SERIES
    : S E R I E S
    ;

DESC
    : D E S C
    ;

ASC
    : A S C
    ;
//============================
// End of the keywords list
//============================
COMMA : ',';

STAR : '*';

OPERATOR_EQ : '=' | '==';

OPERATOR_GT : '>';

OPERATOR_GTE : '>=';

OPERATOR_LT : '<';

OPERATOR_LTE : '<=';

OPERATOR_NEQ : '!=' | '<>';

OPERATOR_IN : I N;

OPERATOR_AND
    : A N D
    | '&'
    | '&&'
    ;

OPERATOR_OR
    : O R
    | '|'
    | '||'
    ;

OPERATOR_NOT
    : N O T | '!'
    ;

OPERATOR_CONTAINS
    : C O N T A I N S
    ;

MINUS : '-';

PLUS : '+';

DIV : '/';

MOD : '%';

DOT : '.';

LR_BRACKET : '(';

RR_BRACKET : ')';

LS_BRACKET : '[';

RS_BRACKET : ']';

L_BRACKET : '{';

R_BRACKET : '}';

UNDERLINE : '_';

NaN : 'NaN';

INF : I N F;

stringLiteral
    : SINGLE_QUOTE_STRING_LITERAL
    | DOUBLE_QUOTE_STRING_LITERAL
    ;

INT : [0-9]+;

INTEGER : [0-9]+I;

// tricky, in order to parse float like "2.56f" instead of "2.56 f"
FLOAT
    : [0-9]+ . [0-9]+ F
    | . [0-9]+ F
    | [0-9]+ . [0-9]+ ('e'|'E') ('+'|'-')? [0-9]+ F
    | . [0-9]+ ('e'|'E') ('+'|'-')? [0-9]+ F
    | [0-9]+ ('e'|'E') ('+'|'-')? [0-9]+ F
    ;

EXPONENT : INT ('e'|'E') ('+'|'-')? INT ;

DURATION
    :
    (INT+ (Y|M O|W|D|H|M|S|M S|U S|N S))+
    ;

DATETIME
    : INT ('-'|'/') INT ('-'|'/') INT
      ((T | WS)
      INT ':' INT ':' INT (DOT INT)?
      (('+' | '-') INT ':' INT)?)?
    ;

/** Allow unicode rule/token names */
ID : FIRST_NAME_CHAR NAME_CHAR*;

fragment
FIRST_NAME_CHAR
    :   'A'..'Z'
    |   'a'..'z'
    |   '0'..'9'
    |   '_'
    |   '/'
    |   '@'
    |   '#'
    |   '$'
    |   '%'
    |   '&'
    |   CN_CHAR
    ;

fragment
NAME_CHAR
    :   'A'..'Z'
    |   'a'..'z'
    |   '0'..'9'
    |   '_'
    |   '-'
    |   ':'
    |   '/'
    |   '@'
    |   '#'
    |   '$'
    |   '%'
    |   '&'
    |   '+'
    |   CN_CHAR
    ;

fragment CN_CHAR
  : '\u2E80'..'\u9FFF'
  ;

DOUBLE_QUOTE_STRING_LITERAL
    : '"' ('\\' . | ~'"' )*? '"'
    ;

SINGLE_QUOTE_STRING_LITERAL
    : '\'' ('\\' . | ~'\'' )*? '\''
    ;

//Characters and write it this way for case sensitivity
fragment A
    : 'a' | 'A'
    ;

fragment B
    : 'b' | 'B'
    ;

fragment C
    : 'c' | 'C'
    ;

fragment D
    : 'd' | 'D'
    ;

fragment E
    : 'e' | 'E'
    ;

fragment F
    : 'f' | 'F'
    ;

fragment G
    : 'g' | 'G'
    ;

fragment H
    : 'h' | 'H'
    ;

fragment I
    : 'i' | 'I'
    ;

fragment J
    : 'j' | 'J'
    ;

fragment K
    : 'k' | 'K'
    ;

fragment L
    : 'l' | 'L'
    ;

fragment M
    : 'm' | 'M'
    ;

fragment N
    : 'n' | 'N'
    ;

fragment O
    : 'o' | 'O'
    ;

fragment P
    : 'p' | 'P'
    ;

fragment Q
    : 'q' | 'Q'
    ;

fragment R
    : 'r' | 'R'
    ;

fragment S
    : 's' | 'S'
    ;

fragment T
    : 't' | 'T'
    ;

fragment U
    : 'u' | 'U'
    ;

fragment V
    : 'v' | 'V'
    ;

fragment W
    : 'w' | 'W'
    ;

fragment X
    : 'x' | 'X'
    ;

fragment Y
    : 'y' | 'Y'
    ;

fragment Z
    : 'z' | 'Z'
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
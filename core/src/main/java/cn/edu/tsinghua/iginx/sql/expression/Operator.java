package cn.edu.tsinghua.iginx.sql.expression;

public enum Operator {

    PLUS,
    MINUS,
    STAR,
    DIV,
    MOD;

    public static String operatorToString(Operator operator) {
        switch (operator) {
            case PLUS:
                return "+";
            case MINUS:
                return "-";
            case STAR:
                return "×";
            case DIV:
                return "÷";
            case MOD:
                return "%";
            default:
                return "";
        }
    }
}

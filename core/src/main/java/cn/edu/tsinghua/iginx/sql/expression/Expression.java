package cn.edu.tsinghua.iginx.sql.expression;

public interface Expression {

    String getColumnName();

    ExpressionType getType();

    enum ExpressionType {
        Bracket,
        Binary,
        Unary,
        Base,
        Constant
    }
}

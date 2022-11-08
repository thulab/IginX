package cn.edu.tsinghua.iginx.sql.expression;

public class ConstantExpression implements Expression {

    private final Object value;

    public ConstantExpression(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String getColumnName() {
        return value.toString();
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.Constant;
    }
}

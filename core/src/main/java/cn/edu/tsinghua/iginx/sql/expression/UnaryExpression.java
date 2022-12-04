package cn.edu.tsinghua.iginx.sql.expression;

public class UnaryExpression implements Expression {

    private final Operator operator;
    private final Expression expression;

    public UnaryExpression(Operator operator, Expression expression) {
        this.operator = operator;
        this.expression = expression;
    }

    public Operator getOperator() {
        return operator;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public String getColumnName() {
        return Operator.operatorToString(operator) + " " + expression.getColumnName();
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.Unary;
    }
}

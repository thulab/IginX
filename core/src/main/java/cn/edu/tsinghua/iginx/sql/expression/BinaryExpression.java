package cn.edu.tsinghua.iginx.sql.expression;

public class BinaryExpression implements Expression {

    private final Expression leftExpression;
    private final Expression rightExpression;
    private final Operator op;

    public BinaryExpression(Expression leftExpression, Expression rightExpression, Operator op) {
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
        this.op = op;
    }

    public Expression getLeftExpression() {
        return leftExpression;
    }

    public Expression getRightExpression() {
        return rightExpression;
    }

    public Operator getOp() {
        return op;
    }

    @Override
    public String getColumnName() {
        return leftExpression.getColumnName() + " " +
            Operator.operatorToString(op) + " " +
            rightExpression.getColumnName();
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.Binary;
    }
}

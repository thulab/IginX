package cn.edu.tsinghua.iginx.sql.expression;

public class BracketExpression implements Expression {

    private final Expression expression;

    public BracketExpression(Expression expression) {
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public String getColumnName() {
        return "(" + expression.getColumnName() + ")";
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.Bracket;
    }
}

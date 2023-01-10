package cn.edu.tsinghua.iginx.engine.shared.function.system;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.PARAM_EXPR;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.expression.BinaryExpression;
import cn.edu.tsinghua.iginx.sql.expression.BracketExpression;
import cn.edu.tsinghua.iginx.sql.expression.ConstantExpression;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.Operator;
import cn.edu.tsinghua.iginx.sql.expression.UnaryExpression;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import java.util.Collections;
import java.util.Map;

public class ArithmeticExpr implements RowMappingFunction {

    public static final String ARITHMETIC_EXPR = "arithmetic_expr";

    private static final ArithmeticExpr INSTANCE = new ArithmeticExpr();

    private ArithmeticExpr() {
    }

    public static ArithmeticExpr getInstance() {
        return INSTANCE;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.System;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.RowMapping;
    }

    @Override
    public String getIdentifier() {
        return ARITHMETIC_EXPR;
    }

    @Override
    public Row transform(Row row, Map<String, Value> params) throws Exception {
        if (params.size() == 0 || params.size() > 2) {
            throw new IllegalArgumentException("unexpected params for arithmetic_expr.");
        }
        Expression expr = (Expression) params.get(PARAM_EXPR).getValue();

        Value ret = calculateExpr(row, expr);
        if (ret == null) {
            return Row.EMPTY_ROW;
        }

        Field targetField = new Field(expr.getColumnName(), ret.getDataType());

        Header header = row.getHeader().hasKey() ?
            new Header(Field.KEY, Collections.singletonList(targetField)) :
            new Header(Collections.singletonList(targetField));

        return new Row(header, row.getKey(), new Object[]{ret.getValue()});
    }

    private Value calculateExpr(Row row, Expression expr) {
        switch (expr.getType()) {
            case Constant:
                return calculateConstantExpr((ConstantExpression) expr);
            case Base:
                return calculateBaseExpr(row, (BaseExpression) expr);
            case Bracket:
                return calculateBracketExpr(row, (BracketExpression) expr);
            case Unary:
                return calculateUnaryExpr(row, (UnaryExpression) expr);
            case Binary:
                return calculateBinaryExpr(row, (BinaryExpression) expr);
            default:
                throw new IllegalArgumentException(String.format("Unknown expr type: %s", expr.getType()));
        }
    }

    private Value calculateConstantExpr(ConstantExpression constantExpr) {
        return new Value(constantExpr.getValue());
    }

    private Value calculateBaseExpr(Row row, BaseExpression baseExpr) {
        String colName = baseExpr.getColumnName();
        int index = row.getHeader().indexOf(colName);
        if (index == -1) {
            return null;
        }
        return new Value(row.getValues()[index]);
    }

    private Value calculateBracketExpr(Row row, BracketExpression bracketExpr) {
        Expression expr = bracketExpr.getExpression();
        return calculateExpr(row, expr);
    }

    private Value calculateUnaryExpr(Row row, UnaryExpression unaryExpr) {
        Expression expr = unaryExpr.getExpression();
        Operator operator = unaryExpr.getOperator();

        Value value = calculateExpr(row, expr);
        if (operator.equals(Operator.PLUS)) {  // positive
            return value;
        }

        switch (value.getDataType()) {  // negative
            case INTEGER:
                return new Value(-value.getIntV());
            case LONG:
                return new Value(-value.getLongV());
            case FLOAT:
                return new Value(-value.getFloatV());
            case DOUBLE:
                return new Value(-value.getDoubleV());
            default:
                return null;
        }
    }

    private Value calculateBinaryExpr(Row row, BinaryExpression binaryExpr) {
        Expression leftExpr = binaryExpr.getLeftExpression();
        Expression rightExpr = binaryExpr.getRightExpression();
        Operator operator = binaryExpr.getOp();

        Value leftVal = calculateExpr(row, leftExpr);
        Value rightVal = calculateExpr(row, rightExpr);

        if (!leftVal.getDataType().equals(rightVal.getDataType())) {  // 两值类型不同，但均为数值类型，转为double再运算
            if (DataTypeUtils.isNumber(leftVal.getDataType()) && DataTypeUtils.isNumber(rightVal.getDataType())) {
                leftVal = ValueUtils.transformToDouble(leftVal);
                rightVal = ValueUtils.transformToDouble(rightVal);
            } else {
                return null;
            }
        }

        switch (operator) {
            case PLUS:
                return calculatePlus(leftVal, rightVal);
            case MINUS:
                return calculateMinus(leftVal, rightVal);
            case STAR:
                return calculateStar(leftVal, rightVal);
            case DIV:
                return calculateDiv(leftVal, rightVal);
            case MOD:
                return calculateMod(leftVal, rightVal);
            default:
                throw new IllegalArgumentException(String.format("Unknown operator type: %s", operator));
        }
    }

    private Value calculatePlus(Value left, Value right) {
        switch (left.getDataType()) {
            case INTEGER:
                return new Value(left.getIntV() + right.getIntV());
            case LONG:
                return new Value(left.getLongV() + right.getLongV());
            case FLOAT:
                return new Value(left.getFloatV() + right.getFloatV());
            case DOUBLE:
                return new Value(left.getDoubleV() + right.getDoubleV());
            default:
                return null;
        }
    }

    private Value calculateMinus(Value left, Value right) {
        switch (left.getDataType()) {
            case INTEGER:
                return new Value(left.getIntV() - right.getIntV());
            case LONG:
                return new Value(left.getLongV() - right.getLongV());
            case FLOAT:
                return new Value(left.getFloatV() - right.getFloatV());
            case DOUBLE:
                return new Value(left.getDoubleV() - right.getDoubleV());
            default:
                return null;
        }
    }

    private Value calculateStar(Value left, Value right) {
        switch (left.getDataType()) {
            case INTEGER:
                return new Value(left.getIntV() * right.getIntV());
            case LONG:
                return new Value(left.getLongV() * right.getLongV());
            case FLOAT:
                return new Value(left.getFloatV() * right.getFloatV());
            case DOUBLE:
                return new Value(left.getDoubleV() * right.getDoubleV());
            default:
                return null;
        }
    }

    private Value calculateDiv(Value left, Value right) {
        switch (left.getDataType()) {
            case INTEGER:
                return new Value(left.getIntV() / right.getIntV());
            case LONG:
                return new Value(left.getLongV() / right.getLongV());
            case FLOAT:
                return new Value(left.getFloatV() / right.getFloatV());
            case DOUBLE:
                return new Value(left.getDoubleV() / right.getDoubleV());
            default:
                return null;
        }
    }

    private Value calculateMod(Value left, Value right) {
        switch (left.getDataType()) {
            case INTEGER:
                return new Value(left.getIntV() % right.getIntV());
            case LONG:
                return new Value(left.getLongV() % right.getLongV());
            case FLOAT:
                return new Value(left.getFloatV() % right.getFloatV());
            case DOUBLE:
                return new Value(left.getDoubleV() % right.getDoubleV());
            default:
                return null;
        }
    }
}

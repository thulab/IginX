package cn.edu.tsinghua.iginx.sql.expression;

public class BaseExpression implements Expression {

    private final String pathName;
    private final String funcName;
    private String alias;

    public BaseExpression(String pathName) {
        this.pathName = pathName;
        this.funcName = "";
        this.alias = "";
    }

    public BaseExpression(String pathName, String funcName) {
        this.pathName = pathName;
        this.funcName = funcName;
        this.alias = "";
    }

    public BaseExpression(String pathName, String funcName, String alias) {
        this.pathName = pathName;
        this.funcName = funcName;
        this.alias = alias;
    }

    public String getPathName() {
        return pathName;
    }

    public String getFuncName() {
        return funcName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String getColumnName() {
        if (hasFunc()) {
            return funcName.toLowerCase() + "(" + pathName + ")";
        } else {
            return pathName;
        }
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.Base;
    }

    public boolean hasFunc() {
        return !funcName.equals("");
    }

    public boolean hasAlias() {
        return !alias.equals("");
    }
}

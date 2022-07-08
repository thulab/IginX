package cn.edu.tsinghua.iginx.sql.statement;

public class Expression {

    private final String pathName;
    private final String funcName;
    private String alias;

    public Expression(String pathName) {
        this.pathName = pathName;
        this.funcName = "";
        this.alias = "";
    }

    public Expression(String pathName, String funcName) {
        this.pathName = pathName;
        this.funcName = funcName;
        this.alias = "";
    }

    public Expression(String pathName, String funcName, String alias) {
        this.pathName = pathName;
        this.funcName = funcName;
        this.alias = alias;
    }

    public boolean hasFunc() {
        return !funcName.equals("");
    }

    public boolean hasAlias() {
        return !alias.equals("");
    }

    public String getColumnName() {
        if (hasFunc()) {
            return funcName.toLowerCase() + "(" + pathName + ")";
        } else {
            return pathName;
        }
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
}

package cn.edu.tsinghua.iginx.exceptions;

public class SQLParserException extends RuntimeException {

    private static final long serialVersionUID = -96743874320267646L;

    public SQLParserException() {
        super("Error format in SQL statement, please check whether SQL statement is correct.");
    }

    public SQLParserException(String message) {
        super(message);
    }

    public SQLParserException(String type, String message) {
        super(String.format("Unsupported type: [%s]. " + message, type));
    }
}

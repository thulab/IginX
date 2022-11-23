package cn.edu.tsinghua.iginx.protocol;

public class ExecutionException extends Exception {

    public ExecutionException(String message) {
        super(message);
    }

    public ExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}

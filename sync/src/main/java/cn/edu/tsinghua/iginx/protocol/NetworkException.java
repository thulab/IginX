package cn.edu.tsinghua.iginx.protocol;

public class NetworkException extends Exception {

    public NetworkException(String message) {
        super(message);
    }

    public NetworkException(String message, Throwable cause) {
        super(message, cause);
    }
}

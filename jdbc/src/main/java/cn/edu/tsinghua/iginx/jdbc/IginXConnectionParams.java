package cn.edu.tsinghua.iginx.jdbc;

public class IginXConnectionParams {

    private String host = Config.IGINX_DEFAULT_HOST;
    private int port = Config.IGINX_DEFAULT_PORT;

    private String username = Config.DEFAULT_USER;
    private String password = Config.DEFAULT_PASSWORD;

    public IginXConnectionParams() {}

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

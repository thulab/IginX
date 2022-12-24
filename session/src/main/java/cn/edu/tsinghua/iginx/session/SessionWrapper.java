package cn.edu.tsinghua.iginx.session;

public interface SessionWrapper {

    /**
     * 增加一个数据源
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @param hasData   数据源数据是否需要被访问
     * @param isReadOnly    数据源是否只读，未来不安排写入数据
     * @return  是否成功
     */
    boolean addDataSource(String ip, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly);

    /**
     * 增加一个数据源，并将其符合模式的表加入系统
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @param hasData   数据源数据是否需要被访问
     * @param isReadOnly    数据源是否只读，未来不安排写入数据
     * @param pattern   表需要符合的模式
     * @return  是否成功
     */
    boolean addTablesFromDataSource(String ip, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly, String pattern);

    /**
     * 增加一个数据源，并将其符合模式的列加入系统
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @param hasData   数据源数据是否需要被访问
     * @param isReadOnly    数据源是否只读，未来不安排写入数据
     * @param pattern   列需要符合的模式
     * @return  是否成功
     */
    boolean addColumnsFromDataSource(String ip, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly, String pattern);

    /**
     * 获取数据源中的相关表格信息
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param user  用户名
     * @param pass  密码
     * @param pattern   表需要符合的模式
     * @return 符合条件的表格信息，装在MetaInfo对象中
     */
    MetaInfo descTableFromDataSource(String ip, int port, String user, String pass, String pattern);

    /**
     * 获取数据源中的相关数据库信息
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param user  用户名
     * @param pass  密码
     * @param pattern   数据库需要符合的模式
     * @return 符合条件的数据库信息，装在MetaInfo对象中
     */
    MetaInfo descDBFromDataSource(String ip, int port, String user, String pass, String pattern);

    /**
     * 获取数据源中的所有表格信息
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param user  用户名
     * @param pass  密码
     * @return 符合条件的数据库信息，装在MetaInfo对象中
     */
    MetaInfo descDataSource(String ip, int port, String user, String pass);

    /**
     *  移除一个数据源
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @return 是否成功
     */
    boolean removeDataSource(String ip, int port, String type, String user, String pass);

}

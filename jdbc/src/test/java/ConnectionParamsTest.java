import cn.edu.tsinghua.iginx.jdbc.Config;
import cn.edu.tsinghua.iginx.jdbc.IginXConnectionParams;
import cn.edu.tsinghua.iginx.jdbc.IginxUrlException;
import cn.edu.tsinghua.iginx.jdbc.Utils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ConnectionParamsTest {

    @Test
    public void testParseURL() throws IginxUrlException {
        String userName = "root";
        String userPwd = "root";
        String host1 = "localhost";
        int port1 = 6888;
        Properties properties = new Properties();
        properties.setProperty(Config.USER, userName);

        properties.setProperty(Config.PASSWORD, userPwd);
        IginXConnectionParams params = Utils.parseUrl(String.format(Config.IGINX_URL_PREFIX + "%s:%s/", host1, port1), properties);
        assertEquals(host1, params.getHost());
        assertEquals(port1, params.getPort());
        assertEquals(userName, params.getUsername());
        assertEquals(userPwd, params.getPassword());

        String host2 = "127.0.0.1";
        int port2 = 6999;
        params = Utils.parseUrl(String.format(Config.IGINX_URL_PREFIX + "%s:%s", host2, port2), properties);
        assertEquals(params.getHost(), host2);
        assertEquals(params.getPort(), port2);
        assertEquals(params.getUsername(), userName);
        assertEquals(params.getPassword(), userPwd);
    }
}

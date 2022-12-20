package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.protocol.NetworkException;
import cn.edu.tsinghua.iginx.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.protocol.zk.ZooKeeperSyncProtocolImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSyncProtocolTest extends SyncProtocolTest {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperSyncProtocolTest.class);

    public static final String CONNECT_STRING = "127.0.0.1:2181";

    @Override
    protected SyncProtocol newSyncProtocol(String category) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STRING)
                .connectionTimeoutMs(15000)
                .retryPolicy(new RetryForever(1000))
                .build();
        client.start();
        try {
            return new ZooKeeperSyncProtocolImpl(category, client, null);
        } catch (NetworkException e) {
            logger.error("[newSyncProtocol] create sync protocol failure: ", e);
        }
        return null;
    }
}

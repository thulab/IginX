package cn.edu.tsinghua.iginx.rest.insert;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SenderManager {
    private static SenderManager instance = new SenderManager();
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final ExecutorService senderPool = Executors.newFixedThreadPool(config.getRestReqSplitNum() * 15);

    private SenderManager() {

    }

    public static SenderManager getInstance() {
        return instance;
    }

    public void addSender(Sender sender) {
        senderPool.execute(sender);
    }
}

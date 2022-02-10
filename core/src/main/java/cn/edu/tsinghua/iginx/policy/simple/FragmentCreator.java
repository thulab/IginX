package cn.edu.tsinghua.iginx.policy.simple;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class FragmentCreator {

    private static Timer timer = new Timer();

    private static final Logger LOGGER = LoggerFactory.getLogger(FragmentCreator.class);
    private final IMetaManager iMetaManager;
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private final SimplePolicy policy;

    public FragmentCreator(SimplePolicy policy, IMetaManager iMetaManager) {
        this.policy = policy;
        this.iMetaManager = iMetaManager;
        init(config.getReAllocatePeriod());
    }

    public boolean waitforUpdate(int version) {
        int retry = config.getRetryCount();
        while (retry > 0) {
            Map<Integer, Integer> timeseriesVersionMap = iMetaManager.getTimeseriesVersionMap();
            Set<Integer> idSet = iMetaManager.getIginxList().stream().map(IginxMeta::getId).
                    map(Long::intValue).collect(Collectors.toSet());
            if (version <= timeseriesVersionMap.entrySet().stream().filter(e -> idSet.contains(e.getKey())).
                    map(Map.Entry::getValue).min(Integer::compareTo).orElse(Integer.MAX_VALUE)) {
                return true;
            }
            LOGGER.info("retry, remain: {}, version:{}, minversion: {}", retry, version, timeseriesVersionMap.values().stream().min(Integer::compareTo).orElse(Integer.MAX_VALUE));
            try {
                Thread.sleep(config.getRetryWait());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            retry --;
        }
        return false;
    }

    public void createFragment() throws Exception {
        LOGGER.info("start CreateFragment");
        if (iMetaManager.election()) {
            int version = iMetaManager.updateVersion();
            if (version > 0) {
                if (!waitforUpdate(version)) {
                    LOGGER.error("update failed");
                    return;
                }
                if (!policy.checkSuccess(iMetaManager.getTimeseriesData())) {
                    policy.setNeedReAllocate(true);
                    LOGGER.info("set ReAllocate true");
                }
            }
        }
        LOGGER.info("end CreateFragment");
    }

    public void init(int length) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    createFragment();
                } catch (Exception e) {
                    LOGGER.error("Error occurs when create fragment", e);
                    e.printStackTrace();
                }
            }
        }, length, length);
    }
}


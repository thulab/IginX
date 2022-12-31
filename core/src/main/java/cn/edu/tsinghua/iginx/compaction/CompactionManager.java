package cn.edu.tsinghua.iginx.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionManager {

    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private static final CompactionManager instance = new CompactionManager();

    public static CompactionManager getInstance() {
        return instance;
    }
}

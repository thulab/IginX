package cn.edu.tsinghua.iginx.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CompactionManager {

    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    private static final List<Compaction> compactionList = new ArrayList<>();

    static {
        compactionList.add(new FragmentDeletionCompaction());
        compactionList.add(new LowWriteFragmentCompaction());
        compactionList.add(new LowAccessFragmentCompaction());
    }

    private static final CompactionManager instance = new CompactionManager();

    public static CompactionManager getInstance() {
        return instance;
    }

    public void clearFragment() throws Exception {
        logger.info("start to compact fragments");
        for (Compaction compaction : compactionList) {
            if (compaction.needCompaction()) {
                compaction.compact();
            }
        }
        logger.info("end compact fragments");
    }
}

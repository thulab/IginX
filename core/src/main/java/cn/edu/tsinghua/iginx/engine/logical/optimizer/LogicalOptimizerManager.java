package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalOptimizerManager {

    private final static Logger logger = LoggerFactory.getLogger(LogicalOptimizerManager.class);

    private final static LogicalOptimizerManager instance = new LogicalOptimizerManager();

    private final static String REMOVE_NOT = "remove_not";

    private final static String FILTER_PUSH_DOWN = "filter_push_down";

    private final static String FILTER_FRAGMENT = "filter_fragment";

    private LogicalOptimizerManager() {
    }

    public static LogicalOptimizerManager getInstance() {
        return instance;
    }

    public Optimizer getOptimizer(String name) {
        if (name == null || name.equals("")) {
            return null;
        }
        logger.info("use {} as logical optimizer.", name);

        switch (name) {
            case REMOVE_NOT:
                return RemoveNotOptimizer.getInstance();
            case FILTER_PUSH_DOWN:
                return FilterPushDownOptimizer.getInstance();
            case FILTER_FRAGMENT:
                return FilterFragmentOptimizer.getInstance();
            default:
                throw new IllegalArgumentException(String.format("unknown logical optimizer: %s", name));
        }
    }
}

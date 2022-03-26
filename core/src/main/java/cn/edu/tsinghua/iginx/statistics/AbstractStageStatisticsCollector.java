package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

public abstract class AbstractStageStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStageStatisticsCollector.class);

    protected static final String BEGIN = "begin";

    protected static final String END = "end";

    private final LinkedBlockingQueue<Statistics> statisticsQueue = new LinkedBlockingQueue<>();

    protected Function<RequestContext, Status> before = requestContext -> {
        requestContext.setExtraParam(BEGIN + getStageName(), System.currentTimeMillis());
        return null;
    };

    protected Function<RequestContext, Status> after = requestContext -> {
        long endTime = System.currentTimeMillis();
        requestContext.setExtraParam(END + getStageName(), endTime);
        long startTime = (long) requestContext.getExtraParam(BEGIN + getStageName());
        statisticsQueue.add(new Statistics(requestContext.getId(), startTime, endTime, requestContext));
        return null;
    };

    public AbstractStageStatisticsCollector() {
        Executors.newSingleThreadExecutor()
            .submit(() -> {
                while (true) {
                    Statistics statistics = statisticsQueue.take();
                    processStatistics(statistics);
                }
            });
    }

    protected abstract String getStageName();

    protected abstract void processStatistics(Statistics statistics);

    public abstract void broadcastStatistics();
}

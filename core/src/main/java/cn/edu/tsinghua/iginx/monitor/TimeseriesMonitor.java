package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeseriesMonitor implements IMonitor {

    private static final Logger logger = LoggerFactory.getLogger(TimeseriesMonitor.class);
    private final boolean isEnableMonitor = ConfigDescriptor.getInstance().getConfig()
            .isEnableMonitor();
    private boolean isStartTimeseriesMonitor = false;
    private final Map<String, Long> timeseriesLoadMap = new ConcurrentHashMap<>(); // 时间序列->总负载
    private static final TimeseriesMonitor instance = new TimeseriesMonitor();

    public static TimeseriesMonitor getInstance() {
        return instance;
    }

    public void start() {
        this.isStartTimeseriesMonitor = true;
    }

    public void stop() {
        this.isStartTimeseriesMonitor = false;
    }

    public Map<String, Long> getTimeseriesLoadMap() {
        return timeseriesLoadMap;
    }

    public void recordAfter(long taskId, TaskExecuteResult result, OperatorType operatorType) {
        try {
            if (isEnableMonitor && isStartTimeseriesMonitor && operatorType == OperatorType.Project) {
                // 构建本次访问的timeseries列表
                List<String> timeseriesList = new ArrayList<>();
                for (Field field : result.getRowStream().getHeader().getFields()) {
                    timeseriesList.add(field.getName());
                }

                long duration = (System.nanoTime() - taskId) / 1000000;
                long averageLoad = duration / timeseriesList.size(); //这里认为范围负载被所有时间序列均分
                for (String timeseries : timeseriesList) {
                    long load = timeseriesLoadMap.getOrDefault(timeseries, 0L);
                    timeseriesLoadMap.put(timeseries, averageLoad + load);
                }
            }
        } catch (PhysicalException e) {
            logger.error("record timeseries error:", e);
        }
    }

    @Override
    public void clear() {
        timeseriesLoadMap.clear();
    }
}
package cn.edu.tsinghua.iginx.monitor;

import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.CopyOnWriteArrayList;

public class NodePerformanceMonitor implements IMonitor {

  private volatile boolean isStart = false;
  private final List<Long> writeLatencyList = new CopyOnWriteArrayList<>();
  private final List<Long> readLatencyList = new CopyOnWriteArrayList<>();
  private static final NodePerformanceMonitor instance = new NodePerformanceMonitor();

  public static NodePerformanceMonitor getInstance() {
    return instance;
  }

  @Override
  public void start() {
    isStart = true;
  }

  @Override
  public void stop() {
    isStart = false;
  }

  public void recordRead(long duration) {
    if (isStart) {
      readLatencyList.add(duration);
    }
  }

  public void recordWrite(long duration) {
    if (isStart) {
      writeLatencyList.add(duration);
    }
  }

  public double getReadLatency() {
    OptionalDouble optionalAverage = readLatencyList.stream().mapToDouble(Number::doubleValue)
        .average();
    if (optionalAverage.isPresent()) {
      return optionalAverage.getAsDouble();
    }
    return 0;
  }

  public double getWriteLatency() {
    OptionalDouble optionalAverage = writeLatencyList.stream().mapToDouble(Number::doubleValue)
        .average();
    if (optionalAverage.isPresent()) {
      return optionalAverage.getAsDouble();
    }
    return 0;
  }
}

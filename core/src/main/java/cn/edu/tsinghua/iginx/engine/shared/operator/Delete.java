package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import java.util.ArrayList;
import java.util.List;

public class Delete extends AbstractUnaryOperator {

  private final List<TimeRange> timeRanges;
  private final List<String> patterns;

  public Delete(FragmentSource source, List<TimeRange> timeRanges, List<String> patterns) {
    super(OperatorType.Delete, source);
    this.timeRanges = timeRanges;
    this.patterns = patterns;
  }

  public List<TimeRange> getTimeRanges() {
    return timeRanges;
  }

  public List<String> getPatterns() {
    return patterns;
  }

  @Override
  public Operator copy() {
    return new Delete((FragmentSource) getSource().copy(), new ArrayList<>(timeRanges),
        new ArrayList<>(patterns));
  }
}

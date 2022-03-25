package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.List;

public class CombineNonQuery extends AbstractMultipleOperator {

  public CombineNonQuery(List<Source> sources) {
    super(OperatorType.CombineNonQuery, sources);
  }

  @Override
  public Operator copy() {
    return new CombineNonQuery(getSources());
  }
}

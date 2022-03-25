package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Select extends AbstractUnaryOperator {

  private Filter filter;

  public Select(Source source, Filter filter) {
    super(OperatorType.Select, source);
    if (filter == null) {
      throw new IllegalArgumentException("filter shouldn't be null");
    }
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @Override
  public Operator copy() {
    return new Select(getSource().copy(), filter.copy());
  }
}

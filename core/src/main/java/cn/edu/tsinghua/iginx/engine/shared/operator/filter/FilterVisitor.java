package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

public interface FilterVisitor {

  void visit(AndFilter filter);

  void visit(OrFilter filter);

  void visit(NotFilter filter);

  void visit(TimeFilter filter);

  void visit(ValueFilter filter);

  void visit(BoolFilter filter);
}

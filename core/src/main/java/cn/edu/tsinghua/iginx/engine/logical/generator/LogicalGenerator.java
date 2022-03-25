package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface LogicalGenerator {

  GeneratorType getType();

  Operator generate(RequestContext ctx);

}

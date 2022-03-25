package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface Optimizer {

  Operator optimize(Operator root);
}

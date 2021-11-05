package cn.edu.tsinghua.iginx.sql.logical;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface Optimizer {
    Operator optimize(Operator root);
}

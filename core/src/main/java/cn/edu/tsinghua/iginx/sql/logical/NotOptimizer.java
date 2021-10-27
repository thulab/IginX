package cn.edu.tsinghua.iginx.sql.logical;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class NotOptimizer implements Optimizer {
    @Override
    public Operator optimize(Operator root) {
        return null;
    }
}

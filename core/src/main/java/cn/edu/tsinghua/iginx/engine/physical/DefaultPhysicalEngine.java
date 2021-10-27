package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class DefaultPhysicalEngine implements PhysicalEngine {
    @Override
    public RowStream execute(Operator root) {
        return null;
    }

    @Override
    public ConstraintManager getConstraintManager() {
        return null;
    }
}

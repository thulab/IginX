package cn.edu.tsinghua.iginx.engine.shared.constraint;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class DefaultConstraintManager implements ConstraintManager {

    @Override
    public boolean check(Operator root) {
        return false;
    }
}

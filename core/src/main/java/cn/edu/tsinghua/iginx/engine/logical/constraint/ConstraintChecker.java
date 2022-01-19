package cn.edu.tsinghua.iginx.engine.logical.constraint;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public interface ConstraintChecker {

    boolean check(Operator root);

}

package cn.edu.tsinghua.iginx.engine.logical.constraint;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class NaiveConstraintChecker implements ConstraintChecker {

    private final static NaiveConstraintChecker instance = new NaiveConstraintChecker();

    private NaiveConstraintChecker() {
    }

    public static NaiveConstraintChecker getInstance() {
        return instance;
    }

    @Override
    public boolean check(Operator root) {
        return true;
    }
}

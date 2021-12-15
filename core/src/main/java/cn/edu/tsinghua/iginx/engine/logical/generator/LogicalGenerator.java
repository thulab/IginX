package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.statement.Statement;

public interface LogicalGenerator {

    GeneratorType getType();

    Operator generate(Statement statement);

}

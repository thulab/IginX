package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.ArrayList;
import java.util.List;

public class AddSchemaPrefix extends AbstractUnaryOperator {

    private final String schemaPrefix;// 可以为 null

    public AddSchemaPrefix(Source source, String schemaPrefix) {
        super(OperatorType.AddSchemaPrefix, source);
        this.schemaPrefix = schemaPrefix;
    }

    @Override
    public Operator copy() {
        return new AddSchemaPrefix(getSource().copy(), schemaPrefix);
    }

    public String getSchemaPrefix() {
        return schemaPrefix;
    }
}

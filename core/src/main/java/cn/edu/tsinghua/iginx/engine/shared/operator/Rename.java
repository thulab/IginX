package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.HashMap;
import java.util.Map;

public class Rename extends AbstractUnaryOperator {

    private final Map<String, String> aliasMap;

    public Rename(Source source, Map<String, String> aliasMap) {
        super(OperatorType.Rename, source);
        if (aliasMap == null) {
            throw new IllegalArgumentException("aliasMap shouldn't be null");
        }
        this.aliasMap = aliasMap;
    }

    public Map<String, String> getAliasMap() {
        return aliasMap;
    }

    @Override
    public Operator copy() {
        return new Rename(getSource().copy(), new HashMap<>(aliasMap));
    }
}

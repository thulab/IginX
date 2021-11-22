package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.data.DataSection;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Insert extends AbstractUnaryOperator {

    private final DataSection data;

    public Insert(Source source, DataSection data) {
        super(OperatorType.Insert, source);
        if (data == null) {
            throw new IllegalArgumentException("raw data shouldn't be null");
        }
        this.data = data;
    }

    public DataSection getData() {
        return data;
    }

    @Override
    public Operator copy() {
        // data should not be copied in memory.
        return new Insert(getSource().copy(), data);
    }
}

package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.data.DataSection;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;

public class Insert extends AbstractUnaryOperator {

    private final InsertType insertType;

    private final DataSection data;

    public Insert(FragmentSource source, DataSection data, InsertType insertType) {
        super(OperatorType.Insert, source);
        if (data == null) {
            throw new IllegalArgumentException("raw data shouldn't be null");
        }
        this.data = data;
        this.insertType = insertType;
    }

    public InsertType getInsertType() {
        return insertType;
    }

    public DataSection getData() {
        return data;
    }

    @Override
    public Operator copy() {
        // data should not be copied in memory.
        return new Insert((FragmentSource) getSource().copy(), data, getInsertType());
    }

    public enum InsertType {
        Column,
        Row,
        NonAlignedColumn,
        NonAlignedRow
    }
}

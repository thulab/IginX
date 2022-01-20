package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;

public class ShowTimeSeries extends AbstractUnaryOperator {

    public ShowTimeSeries(GlobalSource source) {
        super(OperatorType.ShowTimeSeries, source);
    }

    @Override
    public Operator copy() {
        return new ShowTimeSeries((GlobalSource) getSource().copy());
    }
}

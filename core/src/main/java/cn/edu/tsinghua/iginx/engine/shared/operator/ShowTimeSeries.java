package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;

public class ShowTimeSeries extends AbstractUnaryOperator {

    // 允许传入时间序列检索，现在用于du的所有时间序列查询
    private String timeSeriesPrefix;

    public ShowTimeSeries(GlobalSource source,
        String timeSeriesPrefix) {
        super(OperatorType.ShowTimeSeries, source);
        this.timeSeriesPrefix = timeSeriesPrefix;
    }

    @Override
    public Operator copy() {
        return new ShowTimeSeries((GlobalSource) getSource().copy(), timeSeriesPrefix);
    }

    public String getTimeSeriesPrefix() {
        return timeSeriesPrefix;
    }
}

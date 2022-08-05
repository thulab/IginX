package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.sql.statement.ShowTimeSeriesStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowTimeSeriesGenerator extends AbstractGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ShowTimeSeriesGenerator.class);
    private final static ShowTimeSeriesGenerator instance = new ShowTimeSeriesGenerator();

    private ShowTimeSeriesGenerator() {
        this.type = GeneratorType.ShowTimeSeries;
    }

    public static ShowTimeSeriesGenerator getInstance() {
        return instance;
    }

    @Override
    protected Operator generateRoot(Statement statement) {
        ShowTimeSeriesStatement showTimeSeriesStatement = (ShowTimeSeriesStatement) statement;
        return new ShowTimeSeries(
            new GlobalSource(),
            showTimeSeriesStatement.getPathRegexSet(),
            showTimeSeriesStatement.getTagFilter());
    }
}

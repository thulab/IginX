package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SqlParser.*;
import cn.edu.tsinghua.iginx.sql.operator.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.*;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.INFLUXDB;
import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.IOTDB;

public class IginXSqlVisitor extends SqlBaseVisitor<Operator> {
    @Override
    public Operator visitSqlStatement(SqlParser.SqlStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Operator visitInsertStatement(SqlParser.InsertStatementContext ctx) {
        InsertOperator insertOp = new InsertOperator();
        insertOp.setPrefixPath(ctx.path().getText());
        // parse paths
        List<MeasurementNameContext> measurementNames = ctx.insertColumnsSpec().measurementName();
        measurementNames.stream().forEach(e -> insertOp.setPath(e.getText()));
        // parse times, values and types
        parseInsertValuesSpec(ctx.insertValuesSpec(), insertOp);

        if (insertOp.getPaths().size() != insertOp.getValues().length) {
            throw new SQLParserException("Insert path size and value size must be equal.");
        }
        return insertOp;
    }

    @Override
    public Operator visitDeleteStatement(SqlParser.DeleteStatementContext ctx) {
        DeleteOperator deleteOp = new DeleteOperator();
        // parse delete paths
        ctx.path().stream().forEach(e -> deleteOp.addPath(e.getText()));
        // parse time range
        Pair<Long, Long> range = parseTimeRange(ctx.timeRange());
        deleteOp.setStartTime(range.left);
        deleteOp.setEndTime(range.right);
        return deleteOp;
    }

    @Override
    public Operator visitSelectStatement(SqlParser.SelectStatementContext ctx) {
        SelectOperator selectOp = new SelectOperator();
        // Step 1. parse as much information as possible.
        // parse from paths
        if (ctx.fromClause() != null) {
            selectOp.setFromPath(ctx.fromClause().path().getText());
        }
        // parse select paths
        if (ctx.selectClause() != null) {
            parseSelectPaths(ctx.selectClause(), selectOp);
        }
        // parse where clause
        if (ctx.whereClause() != null) {
            // parse time range
            Pair<Long, Long> range = parseTimeRange(ctx.whereClause().timeRange());
            selectOp.setStartTime(range.left);
            selectOp.setEndTime(range.right);

            // parse booleanExpression
            if (ctx.whereClause().orExpression() != null) {
                // can not simply use orExpression().getText()
                // you may get "a>1andb<2orc>3", and value filter may goes wrong.
                String ret = parseOrExpression(ctx.whereClause().orExpression(), selectOp);
                selectOp.setBooleanExpression(ret);
                selectOp.setHasValueFilter(true);
            }
        }
        // parse special clause
        if (ctx.specialClause() != null) {
            parseSpecialClause(ctx.specialClause(), selectOp);
        }

        // Step 2. decide the query type according to the information.
        selectOp.setQueryType();

        return selectOp;
    }

    @Override
    public Operator visitShowReplicationStatement(SqlParser.ShowReplicationStatementContext ctx) {
        return new ShowReplicationOperator();
    }

    @Override
    public Operator visitAddStorageEngineStatement(SqlParser.AddStorageEngineStatementContext ctx) {
        AddStorageEngineOperator addStorageEngineOp = new AddStorageEngineOperator();
        // parse engines
        List<StorageEngineContext> engines = ctx.storageEngineSpec().storageEngine();
        for (StorageEngineContext engine : engines) {
            String ip = engine.ip().getText();
            int port = Integer.parseInt(engine.port.getText());
            StorageEngineType type = parseStorageEngineType(engine.engineType());
            Map<String, String> extra = parseExtra(engine.extra);
            addStorageEngineOp.setEngines(new StorageEngine(ip, port, type, extra));
        }
        return addStorageEngineOp;
    }

    @Override
    public Operator visitCountPointsStatement(SqlParser.CountPointsStatementContext ctx) {
        return new CountPointsOperator();
    }

    @Override
    public Operator visitClearDataStatement(SqlParser.ClearDataStatementContext ctx) {
        return new ClearDataOperator();
    }

    @Override
    public Operator visitShowTimeSeriesStatement(ShowTimeSeriesStatementContext ctx) {
        return new ShowTimeSeriesOperator();
    }

    private void parseSelectPaths(SelectClauseContext ctx, SelectOperator selectOp) {
        List<ExpressionContext> expressions = ctx.expression();

        boolean hasFunc = expressions.get(0).functionName() != null;
        selectOp.setHasFunc(hasFunc);

        for (ExpressionContext expr : expressions) {
            if (expr.functionName() != null && hasFunc) {
                selectOp.setSelectedFuncsAndPaths(SelectOperator.str2FuncType(expr.functionName().getText()), expr.path().getText());
            } else if (expr.functionName() == null && !hasFunc) {
                selectOp.setSelectedFuncsAndPaths(null, expr.path().getText());
            } else {
                throw new SQLParserException("Function modified paths and non-function modified paths can not be mixed");
            }
        }
    }


    private void parseSpecialClause(SpecialClauseContext ctx, SelectOperator selectOp) {
        // parse group by precision
        if (ctx.groupByTimeClause() != null) {
            String duration = ctx.groupByTimeClause().DURATION().getText();
            long precision = TimeUtils.convertDurationStrToLong(0, duration);
            selectOp.setPrecision(precision);
            selectOp.setHasGroupBy(true);
        }
        // parse limit & offset
        // like standard SQL, limit N, M means limit M offset N
        if (ctx.limitClause() != null) {
            if (ctx.limitClause().INT().size() == 1) {
                int limit = Integer.parseInt(ctx.limitClause().INT(0).getText());
                selectOp.setLimit(limit);
                if (ctx.limitClause().offsetClause() != null) {
                    int offset = Integer.parseInt(ctx.limitClause().offsetClause().INT().getText());
                    selectOp.setOffset(offset);
                }
            } else if (ctx.limitClause().INT().size() == 2) {
                int offset = Integer.parseInt(ctx.limitClause().INT(0).getText());
                int limit = Integer.parseInt(ctx.limitClause().INT(1).getText());
                selectOp.setOffset(offset);
                selectOp.setLimit(limit);
            } else {
                throw new SQLParserException("Parse limit clause error. Limit clause should like LIMIT M OFFSET N or LIMIT N, M.");
            }
        }
        // parse order by
        if (ctx.orderByClause() != null) {
            if (selectOp.isHasFunc()) {
                throw new SQLParserException("Not support ORDER BY clause in aggregate query for now.");
            }
            if (ctx.orderByClause().path() != null) {
                String suffixPath = ctx.orderByClause().path().getText();
                selectOp.setOrderByPath(selectOp.getFromPath() + SQLConstant.DOT + suffixPath);
            } else {
                selectOp.setOrderByPath(SQLConstant.TIME);
            }
            if (ctx.orderByClause().DESC() != null) {
                selectOp.setAscending(false);
            }
        }
    }

    private String parseOrExpression(OrExpressionContext ctx, SelectOperator selectOp) {
        List<AndExpressionContext> list = ctx.andExpression();
        if (list.size() > 1) {
            String ret = parseAndExpression(list.get(0), selectOp);
            for (int i = 1; i < list.size(); i++) {
                ret += " || " + parseAndExpression(list.get(i), selectOp);
            }
            return ret;
        } else {
            return parseAndExpression(list.get(0), selectOp);
        }
    }

    private String parseAndExpression(AndExpressionContext ctx, SelectOperator selectOp) {
        List<PredicateContext> list = ctx.predicate();
        if (list.size() > 1) {
            String ret = parsePredicate(list.get(0), selectOp);
            for (int i = 1; i < list.size(); i++) {
                ret += " && " + parsePredicate(list.get(i), selectOp);
            }
            return ret;
        } else {
            return parsePredicate(list.get(0), selectOp);
        }
    }

    private String parsePredicate(PredicateContext ctx, SelectOperator selectOp) {
        if (ctx.orExpression() != null) {
            return "!(" + parseOrExpression(ctx.orExpression(), selectOp) + ")";
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append(selectOp.getFromPath()).append(SQLConstant.DOT).append(ctx.path().getText()).append(" ");
            builder.append(ctx.comparisonOperator().getText()).append(" ");
            builder.append(ctx.constant().getText());
            return builder.toString();
        }
    }

    private StorageEngineType parseStorageEngineType(EngineTypeContext ctx) {
        switch (ctx.getText().toLowerCase()) {
            case SQLConstant.IOT_DB:
                return IOTDB;
            case SQLConstant.INFLUX_DB:
                return INFLUXDB;
            default:
                return null;
        }
    }

    private Map<String, String> parseExtra(StringLiteralContext ctx) {
        Map<String, String> map = new HashMap<>();
        String extra = ctx.getText().trim();
        if (extra.length() == 0 || extra.equals(SQLConstant.DOUBLE_QUOTES)) {
            return map;
        }
        extra = extra.substring(extra.indexOf(SQLConstant.QUOTE) + 1, extra.lastIndexOf(SQLConstant.QUOTE));
        String[] kvStr = extra.split(SQLConstant.COMMA);
        for (String kv : kvStr) {
            String[] kvArray = kv.split(SQLConstant.COLON);
            if (kvArray.length != 2) {
                continue;
            }
            map.put(kvArray[0].trim(), kvArray[1].trim());
        }
        return map;
    }

    private void parseInsertValuesSpec(InsertValuesSpecContext ctx, InsertOperator insertOp) {
        List<InsertMultiValueContext> insertMultiValues = ctx.insertMultiValue();

        int size = insertMultiValues.size();
        int vSize = insertMultiValues.get(0).constant().size();
        long[] times = new long[size];
        Object[][] values = new Object[vSize][size];
        DataType[] types = new DataType[vSize];

        for (int i = 0; i < insertMultiValues.size(); i++) {
            times[i] = parseTime(insertMultiValues.get(i).timeValue());

            List<ConstantContext> constants = insertMultiValues.get(i).constant();
            for (int j = 0; j < constants.size(); j++) {
                values[j][i] = parseValue(constants.get(j));
            }
        }

        // tricky implements, values may be NaN or Null
        int count = 0;
        for (int i = 0; i < insertMultiValues.size(); i++) {
            if (count == types.length) {
                break;
            }
            List<ConstantContext> constants = insertMultiValues.get(i).constant();
            for (int j = 0; j < constants.size(); j++) {
                ConstantContext cons = constants.get(j);
                if (cons.NULL() == null && cons.NaN() == null && types[j] == null) {
                    types[j] = parseType(cons);
                    if (types[j] != null)
                        count++;
                }
            }
        }

        insertOp.setTimes(times);
        insertOp.setValues(values);
        insertOp.setTypes(new ArrayList<>(Arrays.asList(types)));
    }

    private Object parseValue(ConstantContext ctx) {
        if (ctx.booleanClause() != null) {
            return Boolean.parseBoolean(ctx.booleanClause().getText());
        } else if (ctx.dateExpression() != null) {
            return parseDateExpression(ctx.dateExpression());
        } else if (ctx.stringLiteral() != null) {
            // trim, "str" may look like ""str"".
            // Attention!! DataType in thrift interface only! support! binary!
            String str = ctx.stringLiteral().getText();
            return str.substring(1, str.length() - 1).getBytes();
        } else if (ctx.realLiteral() != null) {
            // maybe contains minus, see Sql.g4 for more details.
            return Double.parseDouble(ctx.getText());
        } else if (ctx.FLOAT() != null) {
            String floatStr = ctx.getText();
            return Float.parseFloat(floatStr.substring(0, floatStr.length() - 1));
        } else if (ctx.INT() != null) {
            // INT() may NOT IN [-2147483648, 2147483647], see Sql.g4 for more details.
            return Long.parseLong(ctx.getText());
        } else if (ctx.INTEGER() != null) {
            String intStr = ctx.getText();
            return Integer.parseInt(intStr.substring(0, intStr.length() - 1)); // trim i, 123i —> 123
        } else {
            return null;
        }
    }

    private DataType parseType(ConstantContext ctx) {
        if (ctx.booleanClause() != null) {
            return DataType.BOOLEAN;
        } else if (ctx.dateExpression() != null) {
            // data expression will be auto transform to Long.
            return DataType.LONG;
        } else if (ctx.stringLiteral() != null) {
            return DataType.BINARY;
        } else if (ctx.realLiteral() != null) {
            return DataType.DOUBLE;
        } else if (ctx.FLOAT() != null) {
            return DataType.FLOAT;
        } else if (ctx.INT() != null) {
            // INT() may NOT IN [-2147483648, 2147483647], see Sql.g4 for more details.
            return DataType.LONG;
        } else if (ctx.INTEGER() != null) {
            return DataType.INTEGER;
        } else {
            return null;
        }
    }

    private Pair<Long, Long> parseTimeRange(TimeRangeContext timeRange) {
        long startTime, endTime;

        if (timeRange == null) {
            startTime = Long.MIN_VALUE;
            endTime = Long.MAX_VALUE;
        } else {
            // use index +- 1 to implement [start, end], [start, end),
            // (start, end), (start, end] range in [start, end) interface.
            if (timeRange.timeInterval().LR_BRACKET() != null) { // (
                startTime = parseTime(timeRange.timeInterval().startTime) + 1;
            } else {
                startTime = parseTime(timeRange.timeInterval().startTime);
            }

            if (timeRange.timeInterval().RR_BRACKET() != null) { // )
                endTime = parseTime(timeRange.timeInterval().endTime);
            } else {
                endTime = parseTime(timeRange.timeInterval().endTime) + 1;
            }
        }

        if (startTime > endTime) {
            throw new SQLParserException("Start time should be smaller than endTime in time interval");
        }

        return new Pair<>(startTime, endTime);
    }

    private long parseTime(TimeValueContext time) {
        long ret;
        if (time.INT() != null) {
            ret = Long.parseLong(time.INT().getText());
        } else if (time.dateExpression() != null) {
            ret = parseDateExpression(time.dateExpression());
        } else if (time.dateFormat() != null) {
            ret = parseTimeFormat(time.dateFormat().getText());
        } else if (time.getText().equalsIgnoreCase(SQLConstant.INF)) {
            ret = Long.MAX_VALUE;
        } else {
            ret = Long.MIN_VALUE;
        }
        return ret;
    }

    private long parseDateExpression(DateExpressionContext ctx) {
        long time;
        time = parseTimeFormat(ctx.getChild(0).getText());
        for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
            if (ctx.getChild(i).getText().equals(SQLConstant.PLUS)) {
                time += TimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
            } else {
                time -= TimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
            }
        }
        return time;
    }

    private long parseTimeFormat(String timestampStr) throws SQLParserException {
        if (timestampStr == null || timestampStr.trim().equals(SQLConstant.EMPTY)) {
            throw new SQLParserException("input timestamp cannot be empty");
        }
        if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
            return System.currentTimeMillis();
        }
        try {
            return TimeUtils.convertDatetimeStrToLong(timestampStr);
        } catch (Exception e) {
            throw new SQLParserException(
                    String.format("Input time format %s error. " +
                                    "Input should like yyyy-MM-dd HH:mm:ss." +
                                    "or yyyy/MM/dd HH:mm:ss.",
                            timestampStr));
        }
    }
}

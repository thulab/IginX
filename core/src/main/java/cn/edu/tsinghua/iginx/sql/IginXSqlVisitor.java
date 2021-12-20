package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ConstantContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DateExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertMultiValueContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertValuesSpecContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.MeasurementNameContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.PredicateContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SelectClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowTimeSeriesStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SpecialClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.StorageEngineContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.StringLiteralContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TimeValueContext;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.utils.TimeUtils;

import java.util.*;

public class IginXSqlVisitor extends SqlBaseVisitor<Statement> {
    @Override
    public Statement visitSqlStatement(SqlParser.SqlStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Statement visitInsertStatement(SqlParser.InsertStatementContext ctx) {
        InsertStatement insertStatement = new InsertStatement();
        insertStatement.setPrefixPath(ctx.path().getText());
        // parse paths
        List<MeasurementNameContext> measurementNames = ctx.insertColumnsSpec().measurementName();
        measurementNames.forEach(e -> insertStatement.setPath(e.getText()));
        // parse times, values and types
        parseInsertValuesSpec(ctx.insertValuesSpec(), insertStatement);

        if (insertStatement.getPaths().size() != insertStatement.getValues().length) {
            throw new SQLParserException("Insert path size and value size must be equal.");
        }
        insertStatement.sortData();

        return insertStatement;
    }

    @Override
    public Statement visitDeleteStatement(SqlParser.DeleteStatementContext ctx) {
        DeleteStatement deleteStatement = new DeleteStatement();
        // parse delete paths
        ctx.path().forEach(e -> deleteStatement.addPath(e.getText()));
        // parse time range
        if (ctx.whereClause() != null) {
            Filter filter = parseOrExpression(ctx.whereClause().orExpression(), deleteStatement);
            deleteStatement.setTimeRangesByFilter(filter);
        }
        return deleteStatement;
    }

    @Override
    public Statement visitSelectStatement(SqlParser.SelectStatementContext ctx) {
        SelectStatement selectStatement = new SelectStatement();
        // Step 1. parse as much information as possible.
        // parse from paths
        if (ctx.fromClause() != null) {
            selectStatement.setFromPath(ctx.fromClause().path().getText());
        }
        // parse select paths
        if (ctx.selectClause() != null) {
            parseSelectPaths(ctx.selectClause(), selectStatement);
        }
        // parse where clause
        if (ctx.whereClause() != null) {
            Filter filter = parseOrExpression(ctx.whereClause().orExpression(), selectStatement);
            selectStatement.setFilter(filter);
            selectStatement.setHasValueFilter(true);
        }
        // parse special clause
        if (ctx.specialClause() != null) {
            parseSpecialClause(ctx.specialClause(), selectStatement);
        }

        // Step 2. decide the query type according to the information.
        selectStatement.setQueryType();

        return selectStatement;
    }

    @Override
    public Statement visitDeleteTimeSeriesStatement(SqlParser.DeleteTimeSeriesStatementContext ctx) {
        DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
        ctx.path().forEach(e -> deleteTimeSeriesStatement.addPath(e.getText()));
        return deleteTimeSeriesStatement;
    }

    @Override
    public Statement visitCountPointsStatement(SqlParser.CountPointsStatementContext ctx) {
        return new CountPointsStatement();
    }

    @Override
    public Statement visitClearDataStatement(SqlParser.ClearDataStatementContext ctx) {
        return new ClearDataStatement();
    }

    @Override
    public Statement visitShowReplicationStatement(SqlParser.ShowReplicationStatementContext ctx) {
        return new ShowReplicationStatement();
    }

    @Override
    public Statement visitAddStorageEngineStatement(SqlParser.AddStorageEngineStatementContext ctx) {
        AddStorageEngineStatement addStorageEngineStatement = new AddStorageEngineStatement();
        // parse engines
        List<StorageEngineContext> engines = ctx.storageEngineSpec().storageEngine();
        for (StorageEngineContext engine : engines) {
            String ip = engine.ip().getText();
            int port = Integer.parseInt(engine.port.getText());
            String typeStr = engine.engineType.getText().trim();
            String type = typeStr.substring(typeStr.indexOf(SQLConstant.QUOTE) + 1, typeStr.lastIndexOf(SQLConstant.QUOTE));
            Map<String, String> extra = parseExtra(engine.extra);
            addStorageEngineStatement.setEngines(new StorageEngine(ip, port, type, extra));
        }
        return addStorageEngineStatement;
    }

    @Override
    public Statement visitShowTimeSeriesStatement(ShowTimeSeriesStatementContext ctx) {
        return new ShowTimeSeriesStatement();
    }

    @Override
    public Statement visitShowClusterInfoStatement(SqlParser.ShowClusterInfoStatementContext ctx) {
        return new ShowClusterInfoStatement();
    }

    private void parseSelectPaths(SelectClauseContext ctx, SelectStatement selectStatement) {
        List<ExpressionContext> expressions = ctx.expression();

        for (ExpressionContext expr : expressions) {
            if (expr.functionName() != null) {
                selectStatement.setSelectedFuncsAndPaths(expr.functionName().getText(), expr.path().getText());
            } else {
                selectStatement.setSelectedFuncsAndPaths("", expr.path().getText());
            }
        }

        if (!selectStatement.getFuncTypeSet().isEmpty()) {
            selectStatement.setHasFunc(true);
            Set<String> funcSet = selectStatement.getSelectedFuncsAndPaths().keySet();
            if (funcSet.contains("") && funcSet.size() > 1)
                throw new SQLParserException("Function modified paths and non-function modified paths can not be mixed");
        }
    }


    private void parseSpecialClause(SpecialClauseContext ctx, SelectStatement selectStatement) {
        // parse group by precision
        if (ctx.groupByTimeClause() != null) {
            String duration = ctx.groupByTimeClause().DURATION().getText();
            long precision = TimeUtils.convertDurationStrToLong(0, duration);
            selectStatement.setPrecision(precision);
            selectStatement.setHasGroupBy(true);
        }
        // parse limit & offset
        // like standard SQL, limit N, M means limit M offset N
        if (ctx.limitClause() != null) {
            if (ctx.limitClause().INT().size() == 1) {
                int limit = Integer.parseInt(ctx.limitClause().INT(0).getText());
                selectStatement.setLimit(limit);
                if (ctx.limitClause().offsetClause() != null) {
                    int offset = Integer.parseInt(ctx.limitClause().offsetClause().INT().getText());
                    selectStatement.setOffset(offset);
                }
            } else if (ctx.limitClause().INT().size() == 2) {
                int offset = Integer.parseInt(ctx.limitClause().INT(0).getText());
                int limit = Integer.parseInt(ctx.limitClause().INT(1).getText());
                selectStatement.setOffset(offset);
                selectStatement.setLimit(limit);
            } else {
                throw new SQLParserException("Parse limit clause error. Limit clause should like LIMIT M OFFSET N or LIMIT N, M.");
            }
        }
        // parse order by
        if (ctx.orderByClause() != null) {
            if (selectStatement.hasFunc()) {
                throw new SQLParserException("Not support ORDER BY clause in aggregate query for now.");
            }
            if (ctx.orderByClause().path() != null) {
                String suffixPath = ctx.orderByClause().path().getText();
                String prefixPath = selectStatement.getFromPath();
                String orderByPath = prefixPath + SQLConstant.DOT + suffixPath;
                if (orderByPath.contains("*")) {
                    throw new SQLParserException(String.format("ORDER BY path '%s' has '*', which is not supported.", orderByPath));
                }
                selectStatement.setOrderByPath(orderByPath);
            } else {
                selectStatement.setOrderByPath(SQLConstant.TIME);
            }
            if (ctx.orderByClause().DESC() != null) {
                selectStatement.setAscending(false);
            }
        }
    }

    private Filter parseOrExpression(OrExpressionContext ctx, Statement statement) {
        List<Filter> children = new ArrayList<>();
        for (AndExpressionContext andCtx : ctx.andExpression()) {
            children.add(parseAndExpression(andCtx, statement));
        }
        return new OrFilter(children);
    }

    private Filter parseAndExpression(AndExpressionContext ctx, Statement statement) {
        List<Filter> children = new ArrayList<>();
        for (PredicateContext predicateCtx : ctx.predicate()) {
            children.add(parsePredicate(predicateCtx, statement));
        }
        return new AndFilter(children);
    }

    private Filter parsePredicate(PredicateContext ctx, Statement statement) {
        if (ctx.orExpression() != null) {
            Filter filter = parseOrExpression(ctx.orExpression(), statement);
            return ctx.OPERATOR_NOT() == null ? filter : new NotFilter(filter);
        } else {
            if (ctx.path() == null) {
                return parseTimeFilter(ctx);
            } else {
                StatementType type = statement.getType();
                if (type == StatementType.SELECT) {
                    return parseValueFilter(ctx, (SelectStatement) statement);
                } else {
                    throw new SQLParserException(
                            String.format("[%s] clause can not use value filter.", type.toString().toLowerCase())
                    );
                }
            }
        }
    }

    private TimeFilter parseTimeFilter(PredicateContext ctx) {
        Op op = Op.str2Op(ctx.comparisonOperator().getText());
        // deal with sub clause like 100 < time
        if (ctx.children.get(0) instanceof ConstantContext) {
            op = Op.getDirectionOpposite(op);
        }
        long time = (long) parseValue(ctx.constant());
        return new TimeFilter(op, time);
    }

    private ValueFilter parseValueFilter(PredicateContext ctx, SelectStatement statement) {
        statement.setPathSet(ctx.path().getText());
        String path = statement.getFromPath() + SQLConstant.DOT + ctx.path().getText();
        Op op = Op.str2Op(ctx.comparisonOperator().getText());
        // deal with sub clause like 100 < path
        if (ctx.children.get(0) instanceof ConstantContext) {
            op = Op.getDirectionOpposite(op);
        }
        Value value = new Value(parseValue(ctx.constant()));
        return new ValueFilter(path, op, value);
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

    private void parseInsertValuesSpec(InsertValuesSpecContext ctx, InsertStatement insertStatement) {
        List<InsertMultiValueContext> insertMultiValues = ctx.insertMultiValue();

        int size = insertMultiValues.size();
        int vSize = insertMultiValues.get(0).constant().size();
        Long[] times = new Long[size];
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

        insertStatement.setTimes(new ArrayList<>(Arrays.asList(times)));
        insertStatement.setValues(values);
        insertStatement.setTypes(new ArrayList<>(Arrays.asList(types)));
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

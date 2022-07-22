package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SqlParser.*;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;

public class IginXSqlVisitor extends SqlBaseVisitor<Statement> {

    private final static Set<SelectStatement.FuncType> supportedGroupByLevelFuncSet = new HashSet<>(
        Arrays.asList(
            SelectStatement.FuncType.Sum,
            SelectStatement.FuncType.Count,
            SelectStatement.FuncType.Avg
        )
    );

    @Override
    public Statement visitSqlStatement(SqlStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Statement visitInsertStatement(InsertStatementContext ctx) {
        boolean hasSubQuery = ctx.insertValuesSpec().queryClause() != null;

        InsertStatement insertStatement;
        if (hasSubQuery) {
            insertStatement = new InsertStatement(RawDataType.NonAlignedRow);
        } else {
            insertStatement = new InsertStatement(RawDataType.NonAlignedColumn);
        }

        insertStatement.setPrefixPath(ctx.path().getText());

        if (ctx.tagList() != null) {
            Map<String, String> globalTags = parseTagList(ctx.tagList());
            insertStatement.setGlobalTags(globalTags);
        }
        // parse paths
        ctx.insertColumnsSpec().insertPath().forEach(e -> {
            String path = e.path().getText();
            Map<String, String> tags;
            if (e.tagList() != null) {
                if (insertStatement.hasGlobalTags()) {
                    throw new SQLParserException("Insert path couldn't has global tags and local tags at the same time.");
                }
                tags = parseTagList(e.tagList());
            } else {
                tags = insertStatement.getGlobalTags();
            }
            insertStatement.setPath(path, tags);
        });

        InsertValuesSpecContext valuesSpecContext = ctx.insertValuesSpec();
        if (hasSubQuery) {
            SelectStatement selectStatement = new SelectStatement();
            parseQueryClause(ctx.insertValuesSpec().queryClause(), selectStatement);
            long timeOffset = valuesSpecContext.TIME_OFFSET() == null ? 0 : Long.parseLong(valuesSpecContext.INT().getText());
            return new InsertFromSelectStatement(timeOffset, selectStatement, insertStatement);
        } else {
            // parse times, values and types
            parseInsertValuesSpec(valuesSpecContext, insertStatement);
            if (insertStatement.getPaths().size() != insertStatement.getValues().length) {
                throw new SQLParserException("Insert path size and value size must be equal.");
            }
            insertStatement.sortData();
            return insertStatement;
        }
    }

    @Override
    public Statement visitDeleteStatement(DeleteStatementContext ctx) {
        DeleteStatement deleteStatement = new DeleteStatement();
        // parse delete paths
        ctx.path().forEach(e -> deleteStatement.addPath(e.getText()));
        // parse time range
        if (ctx.whereClause() != null) {
            Filter filter = parseOrExpression(ctx.whereClause().orExpression(), deleteStatement);
            deleteStatement.setTimeRangesByFilter(filter);
        } else {
            List<TimeRange> timeRanges = new ArrayList<>(Collections.singletonList(new TimeRange(0, Long.MAX_VALUE)));
            deleteStatement.setTimeRanges(timeRanges);
        }
        return deleteStatement;
    }

    @Override
    public Statement visitSelectStatement(SelectStatementContext ctx) {
        SelectStatement selectStatement = new SelectStatement();
        if (ctx.queryClause() != null) {
            parseQueryClause(ctx.queryClause(), selectStatement);
        }
        return selectStatement;
    }

    private void parseQueryClause(QueryClauseContext ctx, SelectStatement selectStatement) {
        // Step 1. parse as much information as possible.
        // parse from paths
        if (ctx.fromClause() != null) {
            parseFromPaths(ctx.fromClause(), selectStatement);
        }
        // parse select paths
        if (ctx.selectClause() != null) {
            parseSelectPaths(ctx.selectClause(), selectStatement);
        }
        // parse where clause
        if (ctx.whereClause() != null) {
            Filter filter = parseOrExpression(ctx.whereClause().orExpression(), selectStatement);
            filter = ExprUtils.removeSingleFilter(filter);
            selectStatement.setFilter(filter);
            selectStatement.setHasValueFilter(true);
        }
        // parse with clause
        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseOrTagExpression(ctx.withClause().orTagExpression());
            selectStatement.setTagFilter(tagFilter);
        }
        // parse special clause
        if (ctx.specialClause() != null) {
            parseSpecialClause(ctx.specialClause(), selectStatement);
        }
        // parse as clause
        if (ctx.asClause() != null) {
            parseAsClause(ctx.asClause(), selectStatement);
        }

        // Step 2. decide the query type according to the information.
        selectStatement.setQueryType();
    }

    @Override
    public Statement visitDeleteTimeSeriesStatement(DeleteTimeSeriesStatementContext ctx) {
        DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
        ctx.path().forEach(e -> deleteTimeSeriesStatement.addPath(e.getText()));
        return deleteTimeSeriesStatement;
    }

    @Override
    public Statement visitCountPointsStatement(CountPointsStatementContext ctx) {
        return new CountPointsStatement();
    }

    @Override
    public Statement visitClearDataStatement(ClearDataStatementContext ctx) {
        return new ClearDataStatement();
    }

    @Override
    public Statement visitShowReplicationStatement(ShowReplicationStatementContext ctx) {
        return new ShowReplicationStatement();
    }

    @Override
    public Statement visitAddStorageEngineStatement(AddStorageEngineStatementContext ctx) {
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
    public Statement visitShowClusterInfoStatement(ShowClusterInfoStatementContext ctx) {
        return new ShowClusterInfoStatement();
    }

    private void parseFromPaths(FromClauseContext ctx, SelectStatement selectStatement) {
        if (ctx.queryClause() != null) {
            // parse sub query
            SelectStatement subStatement = new SelectStatement();
            parseQueryClause(ctx.queryClause(), subStatement);
            selectStatement.setSubStatement(subStatement);
        } else {
            List<PathContext> fromPaths = ctx.path();
            for (PathContext fromPath : fromPaths) {
                selectStatement.setFromPath(fromPath.getText());
            }
        }
    }

    @Override
    public Statement visitShowRegisterTaskStatement(ShowRegisterTaskStatementContext ctx) {
        return new ShowRegisterTaskStatement();
    }

    @Override
    public Statement visitRegisterTaskStatement(RegisterTaskStatementContext ctx) {
        String filePath = ctx.filePath.getText();
        filePath = filePath.substring(1, filePath.length() - 1);

        String className = ctx.className.getText();
        className = className.substring(1, className.length() - 1);

        String name = ctx.name.getText();
        name = name.substring(1, name.length() - 1);

        UDFType type = UDFType.TRANSFORM;
        if (ctx.udfType().UDTF() != null) {
            type = UDFType.UDTF;
        } else if (ctx.udfType().UDAF() != null) {
            type = UDFType.UDAF;
        } else if (ctx.udfType().UDSF() != null) {
            type = UDFType.UDSF;
        }
        return new RegisterTaskStatement(name, filePath, className, type);
    }

    @Override
    public Statement visitDropTaskStatement(DropTaskStatementContext ctx) {
        String name = ctx.name.getText();
        name = name.substring(1, name.length() - 1);
        return new DropTaskStatement(name);
    }

    @Override
    public Statement visitCommitTransformJobStatement(CommitTransformJobStatementContext ctx) {
        String path = ctx.filePath.getText();
        path = path.substring(1, path.length() - 1);
        return new CommitTransformJobStatement(path);
    }

    @Override
    public Statement visitShowJobStatusStatement(ShowJobStatusStatementContext ctx) {
        long jobId = Long.parseLong(ctx.jobId.getText());
        return new ShowJobStatusStatement(jobId);
    }

    private void parseSelectPaths(SelectClauseContext ctx, SelectStatement selectStatement) {
        List<ExpressionContext> expressions = ctx.expression();

        boolean hasPrefixPath = !selectStatement.getFromPaths().isEmpty();
        for (ExpressionContext expr : expressions) {
            String funcName = "";
            if (expr.functionName() != null) {
                funcName = expr.functionName().getText();
            }

            String alias = "";
            if (expr.asClause() != null) {
                alias = expr.asClause().ID().getText();
            }

            String selectPath = expr.path().getText();
            if (hasPrefixPath) {
                // need to contact from path
                List<String> fromPaths = selectStatement.getFromPaths();
                for (String fromPath : fromPaths) {
                    String fullPath = fromPath + SQLConstant.DOT + selectPath;
                    Expression expression = new Expression(fullPath, funcName, alias);
                    selectStatement.setSelectedFuncsAndPaths(funcName, expression);
                }
            } else {
                Expression expression = new Expression(selectPath, funcName, alias);
                selectStatement.setSelectedFuncsAndPaths(funcName, expression);
            }
        }

        if (!selectStatement.getFuncTypeSet().isEmpty()) {
            selectStatement.setHasFunc(true);
            Set<String> funcSet = selectStatement.getSelectedFuncsAndExpressions().keySet();
            if (funcSet.contains("") && funcSet.size() > 1)
                throw new SQLParserException("Function modified paths and non-function modified paths can not be mixed");
        }
    }


    private void parseSpecialClause(SpecialClauseContext ctx, SelectStatement selectStatement) {
        if (ctx.groupByClause() != null) {
            // groupByClause = groupByTimeClause + groupByLevelClause
            parseGroupByTimeClause(ctx.groupByClause().timeInterval(), ctx.groupByClause().DURATION(), selectStatement);
            parseGroupByLevelClause(ctx.groupByClause().INT(), selectStatement);
        }
        if (ctx.groupByTimeClause() != null) {
            parseGroupByTimeClause(ctx.groupByTimeClause().timeInterval(), ctx.groupByTimeClause().DURATION(), selectStatement);
        }
        if (ctx.groupByLevelClause() != null) {
            parseGroupByLevelClause(ctx.groupByLevelClause().INT(), selectStatement);
        }
        if (ctx.limitClause() != null) {
            parseLimitClause(ctx.limitClause(), selectStatement);
        }
        if (ctx.orderByClause() != null) {
            parseOrderByClause(ctx.orderByClause(), selectStatement);
        }
    }

    private void parseGroupByTimeClause(TimeIntervalContext timeIntervalContext, TerminalNode duration, SelectStatement selectStatement) {
        String durationStr = duration.getText();
        long precision = TimeUtils.convertDurationStrToLong(0, durationStr);
        Pair<Long, Long> timeInterval = parseTimeInterval(timeIntervalContext);
        selectStatement.setStartTime(timeInterval.k);
        selectStatement.setEndTime(timeInterval.v);
        selectStatement.setPrecision(precision);
        selectStatement.setHasGroupByTime(true);

        // merge value filter and group time range filter
        TimeFilter startTime = new TimeFilter(Op.GE, timeInterval.k);
        TimeFilter endTime = new TimeFilter(Op.L, timeInterval.v);
        Filter mergedFilter;
        if (selectStatement.hasValueFilter()) {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(selectStatement.getFilter(), startTime, endTime)));
        } else {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(startTime, endTime)));
            selectStatement.setHasValueFilter(true);
        }
        selectStatement.setFilter(mergedFilter);
    }

    private void parseGroupByLevelClause(List<TerminalNode> layers, SelectStatement selectStatement) {
        if (!isSupportGroupByLevel(selectStatement)) {
            throw new SQLParserException("Group by level only support aggregate query count, sum, avg for now.");
        }
        layers.forEach(terminalNode -> selectStatement.setLayer(Integer.parseInt(terminalNode.getText())));
    }

    private boolean isSupportGroupByLevel(SelectStatement selectStatement) {
        return supportedGroupByLevelFuncSet.containsAll(selectStatement.getFuncTypeSet());
    }

    // like standard SQL, limit N, M means limit M offset N
    private void parseLimitClause(LimitClauseContext ctx, SelectStatement selectStatement) {
        if (ctx.INT().size() == 1) {
            int limit = Integer.parseInt(ctx.INT(0).getText());
            selectStatement.setLimit(limit);
            if (ctx.offsetClause() != null) {
                int offset = Integer.parseInt(ctx.offsetClause().INT().getText());
                selectStatement.setOffset(offset);
            }
        } else if (ctx.INT().size() == 2) {
            int offset = Integer.parseInt(ctx.INT(0).getText());
            int limit = Integer.parseInt(ctx.INT(1).getText());
            selectStatement.setOffset(offset);
            selectStatement.setLimit(limit);
        } else {
            throw new SQLParserException("Parse limit clause error. Limit clause should like LIMIT M OFFSET N or LIMIT N, M.");
        }
    }

    private void parseOrderByClause(OrderByClauseContext ctx, SelectStatement selectStatement) {
        if (selectStatement.hasFunc()) {
            throw new SQLParserException("Not support ORDER BY clause in aggregate query.");
        }
        if (ctx.path() != null) {
            String orderByPath = ctx.path().getText();
            if (orderByPath.contains("*")) {
                throw new SQLParserException(String.format("ORDER BY path '%s' has '*', which is not supported.", orderByPath));
            }
            selectStatement.setOrderByPath(orderByPath);
        } else {
            selectStatement.setOrderByPath(SQLConstant.TIME);
        }
        if (ctx.DESC() != null) {
            selectStatement.setAscending(false);
        }
    }

    private void parseAsClause(AsClauseContext ctx, SelectStatement selectStatement) {
        String aliasPrefix = ctx.ID().getText();
        selectStatement.getSelectedFuncsAndExpressions().forEach((k, v) -> v.forEach(expression -> {
            String alias = expression.getAlias();
            if (alias.equals("")) {
                alias = aliasPrefix + SQLConstant.DOT + expression.getColumnName();
            } else {
                alias = aliasPrefix + SQLConstant.DOT + alias;
            }
            expression.setAlias(alias);
        }));
    }

    private Pair<Long, Long> parseTimeInterval(TimeIntervalContext interval) {
        long startTime, endTime;

        if (interval == null) {
            startTime = 0;
            endTime = Long.MAX_VALUE;
        } else {
            // use index +- 1 to implement [start, end], [start, end),
            // (start, end), (start, end] range in [start, end) interface.
            if (interval.LR_BRACKET() != null) { // (
                startTime = parseTime(interval.startTime) + 1;
            } else {
                startTime = parseTime(interval.startTime);
            }

            if (interval.RR_BRACKET() != null) { // )
                endTime = parseTime(interval.endTime);
            } else {
                endTime = parseTime(interval.endTime) + 1;
            }
        }

        if (startTime > endTime) {
            throw new SQLParserException("Start time should be smaller than endTime in time interval.");
        }

        return new Pair<>(startTime, endTime);
    }

    private Filter parseOrExpression(OrExpressionContext ctx, Statement statement) {
        List<Filter> children = new ArrayList<>();
        for (AndExpressionContext andCtx : ctx.andExpression()) {
            children.add(parseAndExpression(andCtx, statement));
        }
        return new OrFilter(children);
    }

    private TagFilter parseOrTagExpression(OrTagExpressionContext ctx) {
        List<TagFilter> children = new ArrayList<>();
        for (AndTagExpressionContext andCtx : ctx.andTagExpression()) {
            children.add(parseAndTagExpression(andCtx));
        }
        return new OrTagFilter(children);
    }

    private TagFilter parseAndTagExpression(AndTagExpressionContext ctx) {
        List<TagFilter> children = new ArrayList<>();
        for (TagExpressionContext tagCtx : ctx.tagExpression()) {
            children.add(parseTagExpression(tagCtx));
        }
        return new AndTagFilter(children);
    }

    private TagFilter parseTagExpression(TagExpressionContext ctx) {
        if (ctx.orTagExpression() != null) {
            return parseOrTagExpression(ctx.orTagExpression());
        }
        String tagKey = ctx.tagKey().getText();
        String tagValue = ctx.tagValue().getText();
        return new BaseTagFilter(tagKey, tagValue);
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
            if (ctx.predicatePath().size() == 0) {
                return parseTimeFilter(ctx);
            } else {
                StatementType type = statement.getType();
                if (type != StatementType.SELECT) {
                    throw new SQLParserException(
                        String.format("%s clause can not use value or path filter.", type.toString().toLowerCase())
                    );
                }

                if (ctx.predicatePath().size() == 1) {
                    return parseValueFilter(ctx, (SelectStatement) statement);
                } else {
                    return parsePathFilter(ctx, (SelectStatement) statement);
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

    private Filter parseValueFilter(PredicateContext ctx, SelectStatement statement) {
        PredicatePathContext pathContext = ctx.predicatePath().get(0);
        List<String> pathList = contactFromPathsIfNeeded(statement, pathContext);

        Op op;
        if (ctx.OPERATOR_LIKE() != null) {
            op = Op.LIKE;
        } else {
            op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());
            // deal with sub clause like 100 < path
            if (ctx.children.get(0) instanceof ConstantContext) {
                op = Op.getDirectionOpposite(op);
            }
        }

        Value value;
        if (ctx.regex != null) {
            String regex = ctx.regex.getText();
            value = new Value(regex.substring(1, regex.length()-1));
        } else {
            value = new Value(parseValue(ctx.constant()));
        }


        List<Filter> valueFilterList = new ArrayList<>();
        for (String path: pathList) {
            valueFilterList.add(new ValueFilter(path, op, value));
        }

        return valueFilterList.size() == 1 ?
            valueFilterList.get(0) :
            new AndFilter(valueFilterList);
    }

    private Filter parsePathFilter(PredicateContext ctx, SelectStatement statement) {
        PredicatePathContext pathContextA = ctx.predicatePath().get(0);
        PredicatePathContext pathContextB = ctx.predicatePath().get(1);

        List<String> pathAList = contactFromPathsIfNeeded(statement, pathContextA);
        List<String> pathBList = contactFromPathsIfNeeded(statement, pathContextB);

        Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

        List<Filter> pathFilterList = new ArrayList<>();
        for (String pathA : pathAList) {
            for (String pathB : pathBList) {
                pathFilterList.add(new PathFilter(pathA, op, pathB));
            }
        }

        return pathFilterList.size() == 1 ?
            pathFilterList.get(0) :
            new AndFilter(pathFilterList);
    }

    private List<String> contactFromPathsIfNeeded(SelectStatement statement, PredicatePathContext pathContext) {
        List<String> pathList = new ArrayList<>();
        if (pathContext.INTACT() == null && statement.getSubStatement() == null) {
            for (String fromPath : statement.getFromPaths()) {
                String path = fromPath + SQLConstant.DOT + pathContext.path().getText();
                statement.setPathSet(path);
                pathList.add(path);
            }
        } else {
            statement.setPathSet(pathContext.path().getText());
            pathList.add(pathContext.path().getText());
        }
        return pathList;
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

    private Map<String, String> parseTagList(TagListContext ctx) {
        Map<String, String> tags = new HashMap<>();
        for (TagEquationContext tagCtx : ctx.tagEquation()) {
            String tagKey = tagCtx.tagKey().getText();
            String tagValue = tagCtx.tagValue().getText();
            tags.put(tagKey, tagValue);
        }
        return tags;
    }

}

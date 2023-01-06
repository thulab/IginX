package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SqlParser.*;
import cn.edu.tsinghua.iginx.sql.expression.BinaryExpression;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.expression.BracketExpression;
import cn.edu.tsinghua.iginx.sql.expression.ConstantExpression;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.Expression.ExpressionType;
import cn.edu.tsinghua.iginx.sql.expression.Operator;
import cn.edu.tsinghua.iginx.sql.expression.UnaryExpression;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.join.JoinPart;
import cn.edu.tsinghua.iginx.sql.statement.join.JoinType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.JobState;
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
        // parse tag filter
        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            deleteStatement.setTagFilter(tagFilter);
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
            TagFilter tagFilter = parseWithClause(ctx.withClause());
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
        selectStatement.checkQueryType();
    }

    @Override
    public Statement visitDeleteTimeSeriesStatement(DeleteTimeSeriesStatementContext ctx) {
        DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
        ctx.path().forEach(e -> deleteTimeSeriesStatement.addPath(e.getText()));

        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            deleteTimeSeriesStatement.setTagFilter(tagFilter);
        }
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
            String ipStr = engine.ip.getText();
            String ip = ipStr.substring(ipStr.indexOf(SQLConstant.QUOTE) + 1, ipStr.lastIndexOf(SQLConstant.QUOTE));
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
        ShowTimeSeriesStatement showTimeSeriesStatement = new ShowTimeSeriesStatement();
        for (PathContext pathRegex : ctx.path()) {
            showTimeSeriesStatement.setPathRegex(pathRegex.getText());
        }
        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            showTimeSeriesStatement.setTagFilter(tagFilter);
        }
        if (ctx.limitClause() != null) {
            Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
            showTimeSeriesStatement.setLimit(limitAndOffset.getK());
            showTimeSeriesStatement.setOffset(limitAndOffset.getV());
        }
        return showTimeSeriesStatement;
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
            String fromPath = ctx.path().getText();
            selectStatement.setFromPath(fromPath);
            if (ctx.joinPart() != null && !ctx.joinPart().isEmpty()) {

                selectStatement.setHasJoinParts(true);

                for (JoinPartContext joinPartContext : ctx.joinPart()) {
                    String pathPrefix = joinPartContext.path().getText();
                    if (joinPartContext.join() == null) {  // cross join
                        selectStatement.setJoinPart(new JoinPart(pathPrefix));
                        continue;
                    }

                    JoinType joinType = parseJoinType(joinPartContext.join());

                    Filter filter = null;
                    if (joinPartContext.orExpression() != null) {
                        filter = parseOrExpression(joinPartContext.orExpression(), selectStatement);
                    }

                    List<String> columns = new ArrayList<>();
                    if (joinPartContext.colList() != null && !joinPartContext.colList().isEmpty()) {
                        joinPartContext.colList().path().forEach(pathContext -> columns.add(pathContext.getText()));
                    }

                    selectStatement.setJoinPart(new JoinPart(pathPrefix, joinType, filter, columns));
                }
            }
        }
    }

    private JoinType parseJoinType(JoinContext joinContext) {
        if (joinContext.NATURAL() != null) {
            if (joinContext.LEFT() != null) {
                return JoinType.LeftNatualJoin;
            } else if (joinContext.RIGHT() != null) {
                return JoinType.RightNatualJoin;
            } else {
                return JoinType.InnerNatualJoin;
            }
        } else if (joinContext.LEFT() != null) {
            return JoinType.LeftOuterJoin;
        } else if (joinContext.RIGHT() != null) {
            return JoinType.RightOuterJoin;
        } else if (joinContext.FULL() != null) {
            return JoinType.FullOuterJoin;
        } else {
            return JoinType.InnerJoin;
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

    @Override
    public Statement visitCancelJobStatement(CancelJobStatementContext ctx) {
        long jobId = Long.parseLong(ctx.jobId.getText());
        return new CancelJobStatement(jobId);
    }

    @Override
    public Statement visitShowEligibleJobStatement(ShowEligibleJobStatementContext ctx) {
        JobState jobState = JobState.JOB_UNKNOWN;
        if (ctx.jobStatus().FINISHED() != null) {
            jobState = JobState.JOB_FINISHED;
        } else if (ctx.jobStatus().CREATED() != null) {
            jobState = JobState.JOB_CREATED;
        } else if (ctx.jobStatus().RUNNING() != null) {
            jobState = JobState.JOB_RUNNING;
        } else if (ctx.jobStatus().FAILING() != null) {
            jobState = JobState.JOB_FAILING;
        } else if (ctx.jobStatus().FAILED() != null) {
            jobState = JobState.JOB_FAILED;
        } else if (ctx.jobStatus().CLOSING() != null) {
            jobState = JobState.JOB_CLOSING;
        } else if (ctx.jobStatus().CLOSED() != null) {
            jobState = JobState.JOB_CLOSED;
        }
        return new ShowEligibleJobStatement(jobState);
    }

    private void parseSelectPaths(SelectClauseContext ctx, SelectStatement selectStatement) {
        List<ExpressionContext> expressions = ctx.expression();

        for (ExpressionContext expr : expressions) {
            List<Expression> ret = parseExpression(expr, selectStatement);
            ret.forEach(expression -> {
                if (expression.getType().equals(ExpressionType.Constant)) {
                    // 当select一个不包含在表达式的常量时，这个常量会被看成selectedPath
                    String selectedPath = ((ConstantExpression) expression).getValue().toString();
                    selectStatement.setExpression(parseBaseExpression(selectedPath, selectStatement));
                } else {
                    selectStatement.setExpression(expression);
                }
            });
        }

        if (!selectStatement.getFuncTypeSet().isEmpty()) {
            selectStatement.setHasFunc(true);
        }
    }

    private List<Expression> parseExpression(ExpressionContext ctx, SelectStatement selectStatement) {
        if (ctx.path() != null) {
            return Collections.singletonList(parseBaseExpression(ctx, selectStatement));
        }
        if (ctx.constant() != null) {
            return Collections.singletonList(new ConstantExpression(parseValue(ctx.constant())));
        }

        List<Expression> ret = new ArrayList<>();
        if (ctx.inBracketExpr != null) {
            List<Expression> expressions = parseExpression(ctx.inBracketExpr, selectStatement);
            for (Expression expression : expressions) {
                ret.add(new BracketExpression(expression));
            }
        } else if (ctx.expr != null) {
            List<Expression> expressions = parseExpression(ctx.expr, selectStatement);
            Operator operator = parseOperator(ctx);
            for (Expression expression : expressions) {
                ret.add(new UnaryExpression(operator, expression));
            }
        } else if (ctx.leftExpr != null && ctx.rightExpr != null) {
            List<Expression> leftExpressions = parseExpression(ctx.leftExpr, selectStatement);
            List<Expression> rightExpressions = parseExpression(ctx.rightExpr, selectStatement);
            Operator operator = parseOperator(ctx);
            for (Expression leftExpression : leftExpressions) {
                for (Expression rightExpression : rightExpressions) {
                    ret.add(new BinaryExpression(leftExpression, rightExpression, operator));
                }
            }
        } else {
            throw new SQLParserException("Illegal selected expression");
        }
        return ret;
    }

    private Expression parseBaseExpression(ExpressionContext ctx, SelectStatement selectStatement) {
        String funcName = "";
        if (ctx.functionName() != null) {
            funcName = ctx.functionName().getText();
        }

        String alias = "";
        if (ctx.asClause() != null) {
            alias = ctx.asClause().ID().getText();
        }

        String selectedPath = ctx.path().getText();

        if (!selectStatement.hasJoinParts() && selectStatement.getSubStatement() == null) {
            String fromPath = selectStatement.getFromPath();
            String fullPath = fromPath + SQLConstant.DOT + selectedPath;
            BaseExpression expression = new BaseExpression(fullPath, funcName, alias);
            selectStatement.setSelectedFuncsAndPaths(funcName, expression);
            return expression;
        } else {
            BaseExpression expression = new BaseExpression(selectedPath, funcName, alias);
            selectStatement.setSelectedFuncsAndPaths(funcName, expression);
            return expression;
        }
    }

    private Expression parseBaseExpression(String selectedPath, SelectStatement selectStatement) {
        if (!selectStatement.hasJoinParts() && selectStatement.getSubStatement() == null) {
            String fromPath = selectStatement.getFromPath();
            String fullPath = fromPath + SQLConstant.DOT + selectedPath;
            BaseExpression expression = new BaseExpression(fullPath);
            selectStatement.setSelectedFuncsAndPaths("", expression);
            return expression;
        } else {
            BaseExpression expression = new BaseExpression(selectedPath);
            selectStatement.setSelectedFuncsAndPaths("", expression);
            return expression;
        }
    }

    private Operator parseOperator(ExpressionContext ctx) {
        if (ctx.STAR() != null) {
            return Operator.STAR;
        } else if (ctx.DIV() != null) {
            return Operator.DIV;
        } else if (ctx.MOD() != null) {
            return Operator.MOD;
        } else if (ctx.PLUS() != null) {
            return Operator.PLUS;
        } else if (ctx.MINUS() != null) {
            return Operator.MINUS;
        } else {
            throw new SQLParserException("Unknown operator in expression");
        }
    }

    private void parseSpecialClause(SpecialClauseContext ctx, SelectStatement selectStatement) {
        if (ctx.groupByClause() != null) {
            // groupByClause = groupByTimeClause + groupByLevelClause
            parseGroupByTimeClause(ctx.groupByClause().timeInterval(), ctx.groupByClause().TIME_WITH_UNIT(), selectStatement);
            parseGroupByLevelClause(ctx.groupByClause().INT(), selectStatement);
        }
        if (ctx.groupByTimeClause() != null) {
            parseGroupByTimeClause(ctx.groupByTimeClause().timeInterval(), ctx.groupByTimeClause().TIME_WITH_UNIT(0), selectStatement);
            if (ctx.groupByTimeClause().SLIDE() != null) {
                String slideDistanceStr = ctx.groupByTimeClause().TIME_WITH_UNIT(1).getText();
                long distance = TimeUtils.convertTimeWithUnitStrToLong(0, slideDistanceStr);
                selectStatement.setSlideDistance(distance);
                selectStatement.setHasSlideWindow(true);
            }
        }
        if (ctx.groupByLevelClause() != null) {
            parseGroupByLevelClause(ctx.groupByLevelClause().INT(), selectStatement);
        }
        if (ctx.limitClause() != null) {
            Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
            selectStatement.setLimit(limitAndOffset.getK());
            selectStatement.setOffset(limitAndOffset.getV());
        }
        if (ctx.orderByClause() != null) {
            parseOrderByClause(ctx.orderByClause(), selectStatement);
        }
    }

    private void parseGroupByTimeClause(TimeIntervalContext timeIntervalContext, TerminalNode duration, SelectStatement selectStatement) {
        String durationStr = duration.getText();
        long precision = TimeUtils.convertTimeWithUnitStrToLong(0, durationStr);
        Pair<Long, Long> timeInterval = parseTimeInterval(timeIntervalContext);
        selectStatement.setStartTime(timeInterval.k);
        selectStatement.setEndTime(timeInterval.v);
        selectStatement.setPrecision(precision);
        selectStatement.setSlideDistance(precision);
        selectStatement.setHasGroupByTime(true);
        selectStatement.setHasSlideWindow(false);

        // merge value filter and group time range filter
        KeyFilter startTime = new KeyFilter(Op.GE, timeInterval.k);
        KeyFilter endTime = new KeyFilter(Op.L, timeInterval.v);
        Filter mergedFilter;
        if (selectStatement.hasValueFilter()) {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(selectStatement.getFilter(), startTime, endTime)));
        } else {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(startTime, endTime)));
            selectStatement.setHasValueFilter(true);
        }
        selectStatement.setFilter(mergedFilter);
    }
    
    private void parseSlideWindowByTimeClause(TimeIntervalContext timeIntervalContext, TerminalNode duration, TerminalNode slideDistance, SelectStatement selectStatement) {
        parseGroupByTimeClause(timeIntervalContext, duration, selectStatement);
        String slideDistanceStr = slideDistance.getText();

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
    private Pair<Integer, Integer> parseLimitClause(LimitClauseContext ctx) {
        int limit = Integer.MAX_VALUE;
        int offset = 0;
        if (ctx.INT().size() == 1) {
            limit = Integer.parseInt(ctx.INT(0).getText());
            if (ctx.offsetClause() != null) {
                offset = Integer.parseInt(ctx.offsetClause().INT().getText());
            }
        } else if (ctx.INT().size() == 2) {
            offset = Integer.parseInt(ctx.INT(0).getText());
            limit = Integer.parseInt(ctx.INT(1).getText());
        } else {
            throw new SQLParserException("Parse limit clause error. Limit clause should like LIMIT M OFFSET N or LIMIT N, M.");
        }
        return new Pair<>(limit, offset);
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
            selectStatement.setOrderByPath(SQLConstant.KEY);
        }
        if (ctx.DESC() != null) {
            selectStatement.setAscending(false);
        }
    }

    private void parseAsClause(AsClauseContext ctx, SelectStatement selectStatement) {
        String aliasPrefix = ctx.ID().getText();
        selectStatement.getBaseExpressionMap().forEach((k, v) -> v.forEach(expression -> {
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

    private TagFilter parseWithClause(WithClauseContext ctx) {
        if (ctx.WITHOUT() != null) {
            return new WithoutTagFilter();
        } else if (ctx.orTagExpression() != null) {
            return parseOrTagExpression(ctx.orTagExpression());
        } else {
            return parseOrPreciseExpression(ctx.orPreciseExpression());
        }
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

    private TagFilter parseOrPreciseExpression(OrPreciseExpressionContext ctx) {
        List<BasePreciseTagFilter> children = new ArrayList<>();
        for (AndPreciseExpressionContext tagCtx : ctx.andPreciseExpression()) {
            children.add(parseAndPreciseExpression(tagCtx));
        }
        return new PreciseTagFilter(children);
    }

    private BasePreciseTagFilter parseAndPreciseExpression(AndPreciseExpressionContext ctx) {
        Map<String, String> tagKVMap = new HashMap<>();
        for (PreciseTagExpressionContext tagCtx : ctx.preciseTagExpression()) {
            String tagKey = tagCtx.tagKey().getText();
            String tagValue = tagCtx.tagValue().getText();
            tagKVMap.put(tagKey, tagValue);
        }
        return new BasePreciseTagFilter(tagKVMap);
    }

    private Filter parseOrExpression(OrExpressionContext ctx, Statement statement) {
        List<Filter> children = new ArrayList<>();
        for (AndExpressionContext andCtx : ctx.andExpression()) {
            children.add(parseAndExpression(andCtx, statement));
        }
        return children.size() == 1 ? children.get(0) : new OrFilter(children);
    }

    private Filter parseAndExpression(AndExpressionContext ctx, Statement statement) {
        List<Filter> children = new ArrayList<>();
        for (PredicateContext predicateCtx : ctx.predicate()) {
            children.add(parsePredicate(predicateCtx, statement));
        }
        return children.size() == 1 ? children.get(0) : new AndFilter(children);
    }

    private Filter parsePredicate(PredicateContext ctx, Statement statement) {
        if (ctx.orExpression() != null) {
            Filter filter = parseOrExpression(ctx.orExpression(), statement);
            return ctx.OPERATOR_NOT() == null ? filter : new NotFilter(filter);
        } else {
            if (ctx.path().size() == 0) {
                return parseKeyFilter(ctx);
            } else {
                StatementType type = statement.getType();
                if (type != StatementType.SELECT) {
                    throw new SQLParserException(
                        String.format("%s clause can not use value or path filter.", type.toString().toLowerCase())
                    );
                }

                if (ctx.path().size() == 1) {
                    return parseValueFilter(ctx, (SelectStatement) statement);
                } else {
                    return parsePathFilter(ctx, (SelectStatement) statement);
                }
            }
        }
    }

    private KeyFilter parseKeyFilter(PredicateContext ctx) {
        Op op = Op.str2Op(ctx.comparisonOperator().getText());
        // deal with sub clause like 100 < key
        if (ctx.children.get(0) instanceof ConstantContext) {
            op = Op.getDirectionOpposite(op);
        }
        long time = (long) parseValue(ctx.constant());
        return new KeyFilter(op, time);
    }

    private Filter parseValueFilter(PredicateContext ctx, SelectStatement statement) {
        String path = ctx.path().get(0).getText();
        if (!statement.hasJoinParts() && statement.getSubStatement() == null) {
            path = statement.getFromPath() + SQLConstant.DOT + path;
        }

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

        return new ValueFilter(path, op, value);
    }

    private Filter parsePathFilter(PredicateContext ctx, SelectStatement statement) {
        String pathA = ctx.path().get(0).getText();
        String pathB = ctx.path().get(1).getText();

        Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

        if (!statement.hasJoinParts() && statement.getSubStatement() == null) {
            pathA = statement.getFromPath() + SQLConstant.DOT + pathA;
            pathB = statement.getFromPath() + SQLConstant.DOT + pathB;
        }
        return new PathFilter(pathA, op, pathB);
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
                if (kv.contains("url")) {
                    map.put("url", kv.substring(kv.indexOf(":")+1));
                }
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
        } else if (ctx.INT() != null) {
            // INT() may NOT IN [-2147483648, 2147483647], see Sql.g4 for more details.
            return Long.parseLong(ctx.getText());
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
        } else if (ctx.INT() != null) {
            // INT() may NOT IN [-2147483648, 2147483647], see Sql.g4 for more details.
            return DataType.LONG;
        } else {
            return null;
        }
    }

    private long parseTime(TimeValueContext time) {
        long timeInNs;
        if (time.INT() != null) {
            timeInNs = Long.parseLong(time.INT().getText());
        } else if (time.dateExpression() != null) {
            timeInNs = parseDateExpression(time.dateExpression());
        } else if (time.dateFormat() != null) {
            timeInNs = parseTimeFormat(time.dateFormat());
        } else if (time.getText().equalsIgnoreCase(SQLConstant.INF)) {
            timeInNs = Long.MAX_VALUE;
        } else {
            timeInNs = Long.MIN_VALUE;
        }
        return timeInNs;
    }

    private long parseDateExpression(DateExpressionContext ctx) {
        long time;
        time = parseTimeFormat(ctx.dateFormat());
        for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
            if (ctx.getChild(i).getText().equals(SQLConstant.PLUS)) {
                time += TimeUtils.convertTimeWithUnitStrToLong(time, ctx.getChild(i + 1).getText());
            } else {
                time -= TimeUtils.convertTimeWithUnitStrToLong(time, ctx.getChild(i + 1).getText());
            }
        }
        return time;
    }

    private long parseTimeFormat(DateFormatContext ctx) throws SQLParserException {
        if (ctx.NOW() != null) {
            return System.nanoTime();
        }
        if (ctx.TIME_WITH_UNIT() != null) {
            return TimeUtils.convertTimeWithUnitStrToLong(0, ctx.getText());
        }
        try {
            return TimeUtils.convertDatetimeStrToLong(ctx.getText());
        } catch (Exception e) {
            throw new SQLParserException(String.format("Input time format %s error. ", ctx.getText()));
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

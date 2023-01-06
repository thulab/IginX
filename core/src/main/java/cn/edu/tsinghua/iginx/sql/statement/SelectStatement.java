package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.statement.join.JoinPart;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.*;

public class SelectStatement extends DataStatement {

    private QueryType queryType;

    private boolean hasFunc;
    private boolean hasValueFilter;
    private boolean hasGroupByTime;
    private boolean hasSlideWindow;
    private boolean ascending;
    private boolean hasJoinParts;

    private final List<Expression> expressions;
    private final Map<String, List<BaseExpression>> baseExpressionMap;
    private final Set<FuncType> funcTypeSet;
    private final Set<String> pathSet;
    private String fromPath;
    private final List<JoinPart> joinParts;
    private String orderByPath;
    private Filter filter;
    private TagFilter tagFilter;
    private long precision;
    private long startTime;
    private long endTime;
    private int limit;
    private int offset;
    private long slideDistance;

    private List<Integer> layers;

    private SelectStatement subStatement;

    public SelectStatement() {
        this.statementType = StatementType.SELECT;
        this.queryType = QueryType.Unknown;
        this.ascending = true;
        this.hasJoinParts = false;
        this.expressions = new ArrayList<>();
        this.baseExpressionMap = new HashMap<>();
        this.funcTypeSet = new HashSet<>();
        this.pathSet = new HashSet<>();
        this.joinParts = new ArrayList<>();
        this.orderByPath = "";
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.layers = new ArrayList<>();
        this.subStatement = null;
    }

    // simple query
    public SelectStatement(List<String> paths, long startTime, long endTime) {
        this.queryType = QueryType.SimpleQuery;

        this.pathSet = new HashSet<>();
        this.expressions = new ArrayList<>();
        this.baseExpressionMap = new HashMap<>();
        this.joinParts = new ArrayList<>();
        this.funcTypeSet = new HashSet<>();

        paths.forEach(path -> {
            BaseExpression baseExpression = new BaseExpression(path);
            expressions.add(baseExpression);
            setSelectedFuncsAndPaths("", baseExpression);
        });
        this.hasFunc = false;

        this.setFromSession(startTime, endTime);
    }

    // aggregate query
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType) {
        if (aggregateType == AggregateType.LAST || aggregateType == AggregateType.FIRST) {
            this.queryType = QueryType.LastFirstQuery;
        } else {
            this.queryType = QueryType.AggregateQuery;
        }

        this.pathSet = new HashSet<>();
        this.expressions = new ArrayList<>();
        this.baseExpressionMap = new HashMap<>();
        this.joinParts = new ArrayList<>();
        this.funcTypeSet = new HashSet<>();

        String func = aggregateType.toString().toLowerCase();
        paths.forEach(path -> {
            BaseExpression baseExpression = new BaseExpression(path, func);
            expressions.add(baseExpression);
            setSelectedFuncsAndPaths(func, baseExpression);
        });
        this.hasFunc = true;

        this.setFromSession(startTime, endTime);
    }

    // downSample query
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision) {
        this.queryType = QueryType.DownSampleQuery;

        this.pathSet = new HashSet<>();
        this.expressions = new ArrayList<>();
        this.baseExpressionMap = new HashMap<>();
        this.joinParts = new ArrayList<>();
        this.funcTypeSet = new HashSet<>();

        String func = aggregateType.toString().toLowerCase();
        paths.forEach(path -> {
            BaseExpression baseExpression = new BaseExpression(path, func);
            expressions.add(baseExpression);
            setSelectedFuncsAndPaths(func, baseExpression);
        });
        this.hasFunc = true;

        this.precision = precision;
        this.slideDistance = precision;
        this.startTime = startTime;
        this.endTime = endTime;
        this.hasGroupByTime = true;
        this.hasSlideWindow = false;

        this.setFromSession(startTime, endTime);
    }
    
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision, long slideDistance) {
        this.queryType = QueryType.DownSampleQuery;
        
        this.pathSet = new HashSet<>();
        this.expressions = new ArrayList<>();
        this.baseExpressionMap = new HashMap<>();
        this.joinParts = new ArrayList<>();
        this.funcTypeSet = new HashSet<>();
        
        String func = aggregateType.toString().toLowerCase();
        paths.forEach(path -> {
            BaseExpression baseExpression = new BaseExpression(path, func);
            expressions.add(baseExpression);
            setSelectedFuncsAndPaths(func, baseExpression);
        });
        this.hasFunc = true;
        
        this.precision = precision;
        this.slideDistance = slideDistance;
        this.startTime = startTime;
        this.endTime = endTime;
        this.hasGroupByTime = true;
        this.hasSlideWindow = true;
        
        this.setFromSession(startTime, endTime);
    }

    private void setFromSession(long startTime, long endTime) {
        this.statementType = StatementType.SELECT;

        this.ascending = true;
        this.hasJoinParts = false;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.orderByPath = "";

        this.filter = new AndFilter(new ArrayList<>(Arrays.asList(
            new KeyFilter(Op.GE, startTime),
            new KeyFilter(Op.L, endTime)
        )));
        this.hasValueFilter = true;
        this.layers = new ArrayList<>();
        this.subStatement = null;
    }


    public static FuncType str2FuncType(String str) {
        String identifier = str.toLowerCase();
        switch (identifier) {
            case "first_value":
                return FuncType.FirstValue;
            case "last_value":
                return FuncType.LastValue;
            case "first":
                return FuncType.First;
            case "last":
                return FuncType.Last;
            case "min":
                return FuncType.Min;
            case "max":
                return FuncType.Max;
            case "avg":
                return FuncType.Avg;
            case "count":
                return FuncType.Count;
            case "sum":
                return FuncType.Sum;
            case "":  // no func
                return null;
            default:
                if (FunctionUtils.isRowToRowFunction(identifier)) {
                    return FuncType.Udtf;
                } else if (FunctionUtils.isSetToRowFunction(identifier)) {
                    return FuncType.Udaf;
                } else if (FunctionUtils.isSetToSetFunction(identifier)) {
                    return FuncType.Udsf;
                }
                throw new SQLParserException(String.format("Unregister UDF function: %s.", identifier));
        }
    }

    public boolean hasFunc() {
        return hasFunc;
    }

    public void setHasFunc(boolean hasFunc) {
        this.hasFunc = hasFunc;
    }

    public boolean hasValueFilter() {
        return hasValueFilter;
    }

    public void setHasValueFilter(boolean hasValueFilter) {
        this.hasValueFilter = hasValueFilter;
    }

    public boolean hasGroupByTime() {
        return hasGroupByTime;
    }

    public void setHasGroupByTime(boolean hasGroupByTime) {
        this.hasGroupByTime = hasGroupByTime;
    }
    
    public boolean hasSlideWindow() {
        return hasSlideWindow;
    }
    
    public void setHasSlideWindow(boolean hasSlideWindow) {
        this.hasSlideWindow = hasSlideWindow;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public boolean hasJoinParts() {
        return hasJoinParts;
    }

    public void setHasJoinParts(boolean hasJoinParts) {
        this.hasJoinParts = hasJoinParts;
    }

    public List<String> getSelectedPaths() {
        List<String> paths = new ArrayList<>();
        baseExpressionMap.forEach((k, v) -> {
            v.forEach(expression -> paths.add(expression.getPathName()));
        });
        return paths;
    }

    public Map<String, List<BaseExpression>> getBaseExpressionMap() {
        return baseExpressionMap;
    }

    public void setSelectedFuncsAndPaths(String func, BaseExpression expression) {
        func = func.trim().toLowerCase();

        List<BaseExpression> expressions = this.baseExpressionMap.get(func);
        if (expressions == null) {
            expressions = new ArrayList<>();
            expressions.add(expression);
            this.baseExpressionMap.put(func, expressions);
        } else {
            expressions.add(expression);
        }

        this.pathSet.add(expression.getPathName());

        FuncType type = str2FuncType(func);
        if (type != null) {
            this.funcTypeSet.add(type);
        }
    }

    public Set<FuncType> getFuncTypeSet() {
        return funcTypeSet;
    }

    public Set<String> getPathSet() {
        return pathSet;
    }

    public void setPathSet(String path) {
        this.pathSet.add(path);
    }

    public String getFromPath() {
        return fromPath;
    }

    public void setFromPath(String fromPath) {
        this.fromPath = fromPath;
    }

    public List<JoinPart> getJoinParts() {
        return joinParts;
    }

    public void setJoinPart(JoinPart joinPart) {
        this.joinParts.add(joinPart);
    }

    public String getOrderByPath() {
        return orderByPath;
    }

    public void setOrderByPath(String orderByPath) {
        this.orderByPath = orderByPath;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    public void setTagFilter(TagFilter tagFilter) {
        this.tagFilter = tagFilter;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getPrecision() {
        return precision;
    }

    public void setPrecision(long precision) {
        this.precision = precision;
    }
    
    public long getSlideDistance() {
        return slideDistance;
    }
    
    public void setSlideDistance(long slideDistance) {
        this.slideDistance = slideDistance;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void checkQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public List<Integer> getLayers() {
        return layers;
    }

    public void setLayer(Integer layer) {
        this.layers.add(layer);
    }

    public SelectStatement getSubStatement() {
        return subStatement;
    }

    public void setSubStatement(SelectStatement subStatement) {
        this.subStatement = subStatement;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public void setExpression(Expression expression) {
        expressions.add(expression);
    }

    public Map<String, String> getAliasMap() {
        Map<String, String> aliasMap = new HashMap<>();
        this.baseExpressionMap.forEach((k, v) -> {
            v.forEach(expression -> {
                if (expression.hasAlias()) {
                    String oldName = expression.hasFunc()
                        ? expression.getFuncName().toLowerCase() + "(" + expression.getPathName() + ")"
                        : expression.getPathName();
                    aliasMap.put(oldName, expression.getAlias());
                }
            });
        });
        return aliasMap;
    }

    public void checkQueryType() {
        if (hasFunc) {
            if (hasGroupByTime) {
                this.queryType = QueryType.DownSampleQuery;
            } else {
                this.queryType = QueryType.AggregateQuery;
            }
        } else {
            if (hasGroupByTime) {
                throw new SQLParserException("Group by clause cannot be used without aggregate function.");
            } else {
                this.queryType = QueryType.SimpleQuery;
            }
        }
        if (queryType == QueryType.AggregateQuery) {
            if (funcTypeSet.contains(FuncType.First) || funcTypeSet.contains(FuncType.Last)) {
                this.queryType = QueryType.LastFirstQuery;
            }
        }

        // calculate func type count
        int typeCnt = 0;
        if (funcTypeSet.contains(FuncType.Udtf)) {
            typeCnt++;
        }
        if (funcTypeSet.contains(FuncType.Udaf) || funcTypeSet.contains(FuncType.Min)
            || funcTypeSet.contains(FuncType.Max) || funcTypeSet.contains(FuncType.Sum)
            || funcTypeSet.contains(FuncType.Avg) || funcTypeSet.contains(FuncType.Count)
            || funcTypeSet.contains(FuncType.FirstValue) || funcTypeSet.contains(FuncType.LastValue)) {
            typeCnt++;
        }
        if (funcTypeSet.contains(FuncType.Udsf) || funcTypeSet.contains(FuncType.First)
            || funcTypeSet.contains(FuncType.Last)) {
            typeCnt++;
        }

        // SetToSet SetToRow RowToRow functions can not be mixed.
        if (typeCnt > 1) {
            throw new SQLParserException("SetToSet/SetToRow/RowToRow functions can not be mixed in aggregate query.");
        }
        // SetToSet SetToRow functions and non-function modified path can not be mixed.
        if (typeCnt == 1 && !funcTypeSet.contains(FuncType.Udtf) && baseExpressionMap.containsKey("")) {
            throw new SQLParserException("SetToSet/SetToRow functions and non-function modified path can not be mixed.");
        }
    }

    public enum FuncType {
        Null,
        First,
        Last,
        FirstValue,
        LastValue,
        Min,
        Max,
        Avg,
        Count,
        Sum,
        Udtf,
        Udaf,
        Udsf
    }

    public enum QueryType {
        Unknown,
        SimpleQuery,
        AggregateQuery,
        LastFirstQuery,
        DownSampleQuery,
    }
}

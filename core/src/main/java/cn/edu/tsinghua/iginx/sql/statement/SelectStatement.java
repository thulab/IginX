package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.*;

public class SelectStatement extends DataStatement {

    private QueryType queryType;

    private boolean hasFunc;
    private boolean hasValueFilter;
    private boolean hasGroupByTime;
    private boolean ascending;

    private Map<String, List<Expression>> selectedFuncsAndExpressions;
    private Set<FuncType> funcTypeSet;
    private Set<String> pathSet;
    private List<String> fromPaths;
    private String orderByPath;
    private Filter filter;
    private TagFilter tagFilter;
    private long precision;
    private long startTime;
    private long endTime;
    private int limit;
    private int offset;

    private List<Integer> layers;

    private SelectStatement subStatement;

    public SelectStatement() {
        this.statementType = StatementType.SELECT;
        this.queryType = QueryType.Unknown;
        this.ascending = true;
        this.selectedFuncsAndExpressions = new HashMap<>();
        this.funcTypeSet = new HashSet<>();
        this.pathSet = new HashSet<>();
        this.fromPaths = new ArrayList<>();
        this.orderByPath = "";
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.layers = new ArrayList<>();
        this.subStatement = null;
    }

    // simple query
    public SelectStatement(List<String> paths, long startTime, long endTime) {
        this.queryType = QueryType.SimpleQuery;

        List<Expression> expressions = new ArrayList<>();
        paths.forEach(path -> expressions.add(new Expression(path)));

        this.selectedFuncsAndExpressions = new HashMap<>();
        this.selectedFuncsAndExpressions.put("", expressions);

        this.funcTypeSet = new HashSet<>();

        this.setFromSession(paths, startTime, endTime);
    }

    // aggregate query
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType) {
        if (aggregateType == AggregateType.LAST || aggregateType == AggregateType.FIRST) {
            this.queryType = QueryType.LastFirstQuery;
        } else {
            this.queryType = QueryType.AggregateQuery;
        }

        String func = aggregateType.toString().toLowerCase();
        List<Expression> expressions = new ArrayList<>();
        paths.forEach(path -> expressions.add(new Expression(path, func)));

        this.selectedFuncsAndExpressions = new HashMap<>();
        selectedFuncsAndExpressions.put(func, expressions);

        this.funcTypeSet = new HashSet<>();
        this.funcTypeSet.add(str2FuncType(func));
        this.hasFunc = true;

        this.setFromSession(paths, startTime, endTime);
    }

    // downSample query
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision) {
        this.queryType = QueryType.DownSampleQuery;

        String func = aggregateType.toString().toLowerCase();
        List<Expression> expressions = new ArrayList<>();
        paths.forEach(path -> expressions.add(new Expression(path, func)));

        this.selectedFuncsAndExpressions = new HashMap<>();
        this.selectedFuncsAndExpressions.put(func, expressions);

        this.funcTypeSet = new HashSet<>();
        this.funcTypeSet.add(str2FuncType(func));
        this.hasFunc = true;

        this.precision = precision;
        this.startTime = startTime;
        this.endTime = endTime;
        this.hasGroupByTime = true;

        this.setFromSession(paths, startTime, endTime);
    }

    private void setFromSession(List<String> paths, long startTime, long endTime) {
        this.statementType = StatementType.SELECT;

        this.ascending = true;
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
        this.orderByPath = "";

        this.pathSet = new HashSet<>();
        this.pathSet.addAll(paths);

        this.filter = new AndFilter(new ArrayList<>(Arrays.asList(
            new TimeFilter(Op.GE, startTime),
            new TimeFilter(Op.L, endTime)
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

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public List<String> getSelectedPaths() {
        List<String> paths = new ArrayList<>();
        selectedFuncsAndExpressions.forEach((k, v) -> {
            v.forEach(expression -> paths.add(expression.getPathName()));
        });
        return paths;
    }

    public Map<String, List<Expression>> getSelectedFuncsAndExpressions() {
        return selectedFuncsAndExpressions;
    }

    public void setSelectedFuncsAndPaths(String func, Expression expression) {
        func = func.trim().toLowerCase();


        List<Expression> expressions = this.selectedFuncsAndExpressions.get(func);
        if (expressions == null) {
            expressions = new ArrayList<>();
            expressions.add(expression);
            this.selectedFuncsAndExpressions.put(func, expressions);
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

    public void setFuncTypeSet(Set<FuncType> funcTypeSet) {
        this.funcTypeSet = funcTypeSet;
    }

    public Set<String> getPathSet() {
        return pathSet;
    }

    public void setPathSet(String path) {
        for (String fromPath : fromPaths) {
            String fullPath = fromPath + SQLConstant.DOT + path;
            this.pathSet.add(fullPath);
        }
    }

    public void setIntactPathSet(String path) {
        this.pathSet.add(path);
    }

    public List<String> getFromPaths() {
        return fromPaths;
    }

    public void setFromPath(String fromPath) {
        this.fromPaths.add(fromPath);
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

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
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

    public Map<String, String> getAliasMap() {
        Map<String, String> aliasMap = new HashMap<>();
        this.selectedFuncsAndExpressions.forEach((k, v) -> {
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

    public void setQueryType() {
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
                if (funcTypeSet.size() > 1) {
                    throw new SQLParserException("First/Last query and other aggregate queries can not be mixed.");
                }
            }

            // setToSet setToRow rowToRow functions can not be mixed.
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
            if (typeCnt > 1) {
                throw new SQLParserException("SetToSet/SetToRow/RowToRow functions can not be mixed in aggregate query.");
            }
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

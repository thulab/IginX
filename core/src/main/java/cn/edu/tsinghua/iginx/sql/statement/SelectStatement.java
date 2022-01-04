package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

import java.util.*;

public class SelectStatement extends DataStatement {

    private QueryType queryType;

    private boolean hasFunc;
    private boolean hasValueFilter;
    private boolean hasGroupBy;
    private boolean ascending;

    private Map<String, List<String>> selectedFuncsAndPaths;
    private Set<FuncType> funcTypeSet;
    private Set<String> pathSet;
    private String fromPath;
    private String orderByPath;
    private Filter filter;
    private long precision;
    private long startTime;
    private long endTime;
    private int limit;
    private int offset;

    public SelectStatement() {
        this.statementType = StatementType.SELECT;
        this.queryType = QueryType.Unknown;
        this.ascending = true;
        this.selectedFuncsAndPaths = new HashMap<>();
        this.funcTypeSet = new HashSet<>();
        this.pathSet = new HashSet<>();
        this.fromPath = "";
        this.orderByPath = "";
        this.limit = Integer.MAX_VALUE;
        this.offset = 0;
    }

    // simple query
    public SelectStatement(List<String> paths, long startTime, long endTime) {
        this.queryType = QueryType.SimpleQuery;

        this.selectedFuncsAndPaths = new HashMap<>();
        this.selectedFuncsAndPaths.put("", paths);

        this.funcTypeSet = new HashSet<>();

        this.setFromSession(paths, startTime, endTime);
    }

    // aggregate query
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType) {
        this.queryType = QueryType.AggregateQuery;

        String func = aggregateType.toString().toLowerCase();
        this.selectedFuncsAndPaths = new HashMap<>();
        selectedFuncsAndPaths.put(func, paths);

        this.funcTypeSet = new HashSet<>();
        this.funcTypeSet.add(str2FuncType(func));
        this.hasFunc = true;

        this.setFromSession(paths, startTime, endTime);
    }

    // downSample query
    public SelectStatement(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision) {
        this.queryType = QueryType.DownSampleQuery;

        String func = aggregateType.toString().toLowerCase();
        this.selectedFuncsAndPaths = new HashMap<>();
        this.selectedFuncsAndPaths.put(func, paths);

        this.funcTypeSet = new HashSet<>();
        this.funcTypeSet.add(str2FuncType(func));
        this.hasFunc = true;

        this.precision = precision;
        this.startTime = startTime;
        this.endTime = endTime;
        this.hasGroupBy = true;

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
    }

    public static FuncType str2FuncType(String str) {
        switch (str.toLowerCase()) {
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
            case "":
                return null;
            default:
                return FuncType.Udf;
        }
    }

    public static AggregateType funcType2AggregateType(FuncType type) {
        switch (type) {
            case First:
                return AggregateType.FIRST;
            case Last:
                return AggregateType.LAST;
            case FirstValue:
                return AggregateType.FIRST_VALUE;
            case LastValue:
                return AggregateType.LAST_VALUE;
            case Min:
                return AggregateType.MIN;
            case Max:
                return AggregateType.MAX;
            case Avg:
                return AggregateType.AVG;
            case Count:
                return AggregateType.COUNT;
            case Sum:
                return AggregateType.SUM;
            default:
                return null;
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

    public boolean hasGroupBy() {
        return hasGroupBy;
    }

    public void setHasGroupBy(boolean hasGroupBy) {
        this.hasGroupBy = hasGroupBy;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public List<String> getSelectedPaths() {
        List<String> paths = new ArrayList<>();
        selectedFuncsAndPaths.forEach((k, v) -> {
            paths.addAll(v);
        });
        return paths;
    }

    public Map<String, List<String>> getSelectedFuncsAndPaths() {
        return selectedFuncsAndPaths;
    }

    public void setSelectedFuncsAndPaths(String func, String path) {
        func = func.toLowerCase();
        String fullPath = fromPath + SQLConstant.DOT + path;
        List<String> pathList = this.selectedFuncsAndPaths.get(func);
        if (pathList == null) {
            pathList = new ArrayList<>();
            pathList.add(fullPath);
            this.selectedFuncsAndPaths.put(func, pathList);
        } else {
            pathList.add(fullPath);
        }

        FuncType type = str2FuncType(func);
        if (type != null) {
            this.funcTypeSet.add(type);
        }

        this.pathSet.add(fullPath);
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
        String fullPath = fromPath + SQLConstant.DOT + path;
        this.pathSet.add(fullPath);
    }

    public String getFromPath() {
        return fromPath;
    }

    public void setFromPath(String path) {
        this.fromPath = path;
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

    public void setQueryType() {
        if (hasFunc) {
            if (hasGroupBy && hasValueFilter) {
                this.queryType = QueryType.MixedQuery;
            } else if (hasGroupBy) {
                if (funcTypeSet.size() > 1) {
                    this.queryType = QueryType.MultiDownSampleQuery;
                } else {
                    this.queryType = QueryType.DownSampleQuery;
                }
            } else if (hasValueFilter) {
                this.queryType = QueryType.ValueFilterAndAggregateQuery;
            } else {
                if (funcTypeSet.size() > 1) {
                    this.queryType = QueryType.MultiAggregateQuery;
                } else {
                    this.queryType = QueryType.AggregateQuery;
                }
            }
        } else {
            if (hasGroupBy && hasValueFilter) {
                this.queryType = QueryType.ValueFilterAndDownSampleQuery;
            } else if (hasGroupBy) {
                throw new SQLParserException("Group by clause cannot be used without aggregate function.");
            } else if (hasValueFilter) {
                this.queryType = QueryType.ValueFilterQuery;
            } else {
                this.queryType = QueryType.SimpleQuery;
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
        Udf,  // not support for now.
    }

    public enum QueryType {
        Unknown,
        SimpleQuery,
        AggregateQuery,
        MultiAggregateQuery,
        DownSampleQuery,
        MultiDownSampleQuery,
        ValueFilterQuery,
        ValueFilterAndDownSampleQuery,
        ValueFilterAndAggregateQuery,
        MixedQuery,
    }
}

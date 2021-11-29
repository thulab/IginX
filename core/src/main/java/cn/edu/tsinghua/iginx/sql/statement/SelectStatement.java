package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
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
    private int limit;
    private int offset;

    public SelectStatement() {
        statementType = StatementType.SELECT;
        queryType = QueryType.Unknown;
        ascending = true;
        selectedFuncsAndPaths = new HashMap<>();
        funcTypeSet = new HashSet<>();
        pathSet = new HashSet<>();
        fromPath = "";
        orderByPath = "";
        limit = Integer.MAX_VALUE;
        offset = 0;
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

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("Select statement can not be executed directly.");
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
